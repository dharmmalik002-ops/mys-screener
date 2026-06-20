import { useCallback, useEffect, useMemo, useRef, useState } from "react";
import { createPortal } from "react-dom";
import {
  getChart,
  getJournalData,
  saveJournalData,
  type MarketKey,
  runAiJournalReview,
  type AiJournalReview,
  type IndustryGroupsResponse,
  type IndustryGroupRankItem,
  type IndustryGroupStockItem,
} from "../lib/api";
import { notifyJournalUpdated } from "../lib/journal";
import {
  computeCharges, breakevenPct, DEFAULT_CHARGES, CHARGE_LABELS,
  type ChargesConfig, type ChargesBreakdown, type Product,
} from "../lib/chargesCalculator";
import { NewsModal } from "./NewsModal";
import "./TradeJournalPanel.css";

// ─── Types ─────────────────────────────────────────────────────────────────────

type OpenPosCat = "full" | "half" | "quarter";
interface VCP { t?: string; depth?: string; vol?: string; }
interface Trade {
  symbol: string; type: string; qty: number; price: number; date: string;
  setupType: string; stoploss: number; target: number; tags: string[];
  remarks: string; img?: string; vcp?: VCP; product?: Product;
}
interface PosMeta { cmp?: number; sl?: number; fetchTicker?: string; prev_close?: number; }
interface OpenLot {
  qty: number;
  price: number;
  date: string;
  buyIndex: number;
}
interface ClosedTrade {
  symbol: string; qty: number; entryPx: number; exitPx: number;
  entryDate: string; exitDate: string; pnl: number; perc: number;
  setupType: string; tags: string[]; remarks: string; img?: string; vcp?: VCP;
  stoploss?: number; target?: number;
  equitySnapshot: number; posSizePct: number; sellIndex: number; buyIndices: number[];
  product: Product; grossPnl: number; charges: number; breakdown: ChargesBreakdown;
}
interface OpenPosition {
  symbol: string; qty: number; avgPx: number; totalInvested: number;
  buyIndices: number[]; tags: string[]; remarks: string; img?: string; setupType?: string;
  lots: OpenLot[];
}
interface FIFOResult {
  closedTrades: ClosedTrade[]; openPositions: OpenPosition[];
  currentEquity: number; openLotsDict: Record<string, Trade[]>;
}
type GroupVerdict = "leader" | "constructive" | "watch" | "unknown";
type OpenPositionGroupAnalysis = {
  symbol: string;
  groupName: string;
  parentSector: string;
  groupRank: number | null;
  groupRankChange1w: number | null;
  groupRankChange1m: number | null;
  groupRankChange3m: number | null;
  groupReturn1w: number | null;
  groupReturn1m: number | null;
  groupReturn6m: number | null;
  stockRank: number | null;
  stockCount: number;
  stockReturn1w: number | null;
  stockReturn1m: number | null;
  stockReturn6m: number | null;
  rsRating: number | null;
  exposurePct: number;
  verdict: GroupVerdict;
  note: string;
};

export interface JournalAddRequest {
  symbol: string;
  suggestedPrice?: number;
  suggestedStopLoss?: number;
  setup?: string;
}

// ─── Props ──────────────────────────────────────────────────────────────────────
interface TradeJournalPanelProps {
  market?: MarketKey;
  addRequest?: JournalAddRequest | null;
  onAddRequestHandled?: () => void;
  onOpenSymbolChart?: (symbol: string) => void;
  groupsData?: IndustryGroupsResponse | null;
}

// ─── Constants ─────────────────────────────────────────────────────────────────
const LS_DATA = "tradingJournalData";
const LS_EQUITY = "tradingJournalEquity";
const LS_SETUPS = "tradingJournalSetups";
const LS_POSITIONS = "tradingJournalPositions";
const LS_META = "tradingJournalPosMeta";
const LS_CHARGES = "tradingJournalChargesConfig";

const PREDEFINED_TAGS = [
  "FOMO", "Early Entry", "Late Entry", "Perfect Entry", "Chased",
  "Held Well", "Sold Early", "Held Too Long", "Averaged Down",
  "Followed Plan", "Broke Plan", "Emotional", "Clean Pullback",
  "Strong Volume", "Weak Volume", "Below 50 SMA", "Above 50/200 SMA",
  "10 EMA Support", "21 EMA Support", "Poor Structure",
];
const DEFAULT_SETUPS = ["10 EMA Pullback", "21 EMA Pullback", "Flag", "Breakout", "Low Cheat", "Cheat", "Reversal", "Other"];
// Old built-in setups to retire from the dropdown (existing trades keep their stored value).
const LEGACY_SETUPS = new Set(["bread & butter", "vcp", "flat base", "cup & handle", "pullback", "stage 2", "breakout pullback"]);
const MISTAKE_TAGS = ["FOMO", "Early Entry", "Late Entry", "Chased", "Sold Early", "Held Too Long", "Averaged Down", "Broke Plan", "Emotional", "Below 50 SMA", "Poor Structure", "Weak Volume"];
const QUALITY_GOOD_TAGS = ["Followed Plan", "Perfect Entry", "Held Well", "Clean Pullback", "Strong Volume", "Above 50/200 SMA", "10 EMA Support", "21 EMA Support"];
const QUALITY_BAD_TAGS = ["FOMO", "Chased", "Broke Plan", "Averaged Down", "Emotional", "Below 50 SMA", "Poor Structure", "Weak Volume", "Late Entry"];
const MODEL_SETUP_WORDS = ["bread", "pullback", "stage 2", "10 ema", "21 ema", "vcp", "flat base", "breakout"];

// Plain-English coaching for every Edge Analytics metric: what it measures, how
// to push it the right way, and what that does to your trading. Keyed to the
// clickable metric tiles in the Edge Analytics grid.
type EdgeGlossaryEntry = { title: string; what: string; improve: string; impact: string };
const EDGE_GLOSSARY: Record<string, EdgeGlossaryEntry> = {
  expectancy: {
    title: "Expectancy / trade",
    what: "The average % you make (or lose) on a typical trade, blending your win rate with the size of your wins and losses. It's the single best 'do I have an edge?' number — positive means the system makes money over many trades, negative means it bleeds no matter how it feels.",
    improve: "Cut losers faster so the average loss shrinks, and let winners run to lift the average win. Often the fastest lever is dropping your worst setup entirely — review which setups have negative expectancy and stop taking them.",
    impact: "Raising expectancy is what compounds your account. Even +0.2% per trade over hundreds of trades is the difference between a flat year and a great one; it lets you size up with confidence because you know each trade is worth taking.",
  },
  profit_factor: {
    title: "Profit factor",
    what: "Gross profit divided by gross loss — how many rupees you make for every rupee you lose. 1.0 is breakeven, above 1.5 is a solid edge, above 2.0 is excellent.",
    improve: "Reduce the total bled on losers (tighter stops, no averaging down, exit broken setups) and avoid giving back open profit. One oversized loss can wreck this ratio, so consistent position sizing matters more than picking more winners.",
    impact: "A higher profit factor means a smoother equity curve and shallower drawdowns, so you can stay in the game psychologically and financially through losing streaks.",
  },
  payoff: {
    title: "Payoff (avg win / avg loss)",
    what: "How big your average winner is versus your average loser. A payoff of 2 means your wins are twice the size of your losses — so you can be right less than half the time and still make money.",
    improve: "Stop cutting winners early (use trailing stops or a partial-sell-and-hold plan) and keep losses small and uniform. Don't move your stop lower 'to give it room' — that shrinks payoff fast.",
    impact: "A strong payoff frees you from needing a high win rate. It makes your results robust: you survive choppy periods because the few big wins more than cover the many small losses.",
  },
  avg_win_loss: {
    title: "Avg win / Avg loss",
    what: "The typical size of a winning trade and a losing trade, in %. The gap between them (together with win rate) is your whole edge.",
    improve: "Keep average loss tightly controlled near your planned risk (3–4%). Grow average win by holding leaders through normal pullbacks instead of selling on the first red day.",
    impact: "Tightening the loss side is usually higher-leverage than chasing bigger wins — it directly protects capital and steadies the equity curve.",
  },
  max_dd: {
    title: "Max drawdown (realized)",
    what: "The deepest peak-to-trough fall in your closed-trade equity. It's the worst pain the system has actually put you through, and a preview of what you must be able to stomach.",
    improve: "Lower per-trade risk, avoid clustering correlated positions, and step down size during losing streaks or weak market breadth. Honour your stops — most large drawdowns come from one or two trades held too long.",
    impact: "A shallower max drawdown means you can use larger position sizes safely and you're far less likely to abandon the system at its low point — which is exactly when most traders quit a winning method.",
  },
  streak: {
    title: "Best / worst streak",
    what: "Your longest run of consecutive wins and consecutive losses. The worst-loss streak tells you the psychological and financial endurance the system demands.",
    improve: "You can't engineer streaks directly, but trading only A+ setups and respecting market regime (don't force trades in a weak tape) shortens losing runs. Pre-decide a 'circuit breaker' (e.g. pause after N losses).",
    impact: "Knowing your realistic worst streak lets you size so that streak can't blow up your account — and prepares you mentally so a normal cold patch doesn't make you over-trade or revenge-trade.",
  },
  loss_breaches: {
    title: "Position loss breaches (>4% / >6%)",
    what: "How often a single trade lost more than 4% / 6% of its position value — i.e. where you broke your own stop-loss discipline.",
    improve: "Set the stop before entering, size the position so the stop equals your planned risk, and exit the moment it's hit — no negotiating. Zero breaches is the goal.",
    impact: "Eliminating breaches is the highest-impact fix for most traders: a handful of oversized losses is what turns a profitable system unprofitable. Plug this and your expectancy and drawdown both improve immediately.",
  },
  best_worst: {
    title: "Best / worst trade",
    what: "Your single largest winner and largest loser by %. The worst trade is a discipline check; the best shows what's possible when you let a leader run.",
    improve: "If your worst trade dwarfs your typical loss, that's a stop-discipline or sizing problem to fix. If your best trades are small, you're likely selling winners too early.",
    impact: "Shrinking the worst and growing the best widens your payoff and protects the account from a single catastrophic trade — the kind that sets you back months.",
  },
  avg_r: {
    title: "Avg R / trade",
    what: "Your average result measured in 'R', where 1R is the amount you risked on a trade (entry to stop). +0.5R average means you net half your risk per trade — a clean, size-independent measure of skill.",
    improve: "Add a stop to every trade so R can be measured, take trades with at least 2–3R potential, and let winners reach those targets. Avoid trades where the logical stop is far away (poor R:R).",
    impact: "Thinking in R standardises your trading and decouples it from rupee swings. A positive avg R that holds across position sizes means you can scale the account up safely.",
  },
  r_multiples: {
    title: "2R wins / -1R losses",
    what: "How many trades reached 2× your risk versus how many hit the full -1R stop. The ratio shows whether your winners are actually paying for your losers.",
    improve: "Enter near support so your stop is tight (more 2R headroom), and hold to the 2R target instead of scratching trades at breakeven out of fear. Cut the -1R losers exactly at stop, never beyond.",
    impact: "More 2R wins per -1R loss is the mathematical core of a trend-following edge — it's how a ~40% win rate still compounds strongly.",
  },
  avg_planned_risk: {
    title: "Avg planned risk",
    what: "The average % of the position you intended to risk (entry-to-stop distance) across trades that had a stop. Your playbook targets 3–4%.",
    improve: "Keep planned risk consistent and within your rule. If it's drifting high, your stops are too loose or entries too far from support; tighten entries rather than widening stops.",
    impact: "Consistent, controlled planned risk makes every other metric trustworthy and keeps any single trade from doing outsized damage — the foundation of survivable position sizing.",
  },
  stop_plan_pct: {
    title: "Trades with stop plan",
    what: "The share of trades where you logged a stop before entry. Above 80% is disciplined; below 50% means you're often trading without a defined risk.",
    improve: "Make 'no stop, no trade' a hard rule — record the stop in the journal at entry. Aim to take this number to 100%.",
    impact: "Pre-planned stops are the difference between a controlled loss and a hope-and-pray hold. Pushing this to 100% removes the trades that most often become account-damaging breaches.",
  },
};

function withDefaultSetups(saved: string[]): string[] {
  const seen = new Set<string>();
  return [...DEFAULT_SETUPS, ...(Array.isArray(saved) ? saved : [])].filter((setup) => {
    const key = String(setup || "").trim();
    const lower = key.toLowerCase();
    if (!key || seen.has(lower) || LEGACY_SETUPS.has(lower)) return false;
    seen.add(lower);
    return true;
  });
}

function formatAiReviewText(value: unknown): string {
  if (value === null || value === undefined) return "";
  if (typeof value === "string" || typeof value === "number" || typeof value === "boolean") {
    return String(value);
  }
  if (Array.isArray(value)) {
    return value.map(formatAiReviewText).filter(Boolean).join("; ");
  }
  if (typeof value === "object") {
    const record = value as Record<string, unknown>;
    const strength = formatAiReviewText(record.strength);
    const evidence = formatAiReviewText(record.evidence);
    const mistake = formatAiReviewText(record.mistake);
    const fix = formatAiReviewText(record.fix);
    const action = formatAiReviewText(record.action);
    const read = formatAiReviewText(record.read);
    const parts = [strength || mistake || fix || action || read, evidence].filter(Boolean);
    if (parts.length) return parts.join(" — ");
    return Object.entries(record)
      .map(([key, item]) => {
        const text = formatAiReviewText(item);
        return text ? `${key}: ${text}` : "";
      })
      .filter(Boolean)
      .join("; ");
  }
  return "";
}

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

function holdDays(entryDate: string, exitDate: string): number {
  const entry = getSafeTime(entryDate);
  const exit = getSafeTime(exitDate);
  if (!entry || !exit) return 0;
  return Math.max(0, Math.round((exit - entry) / 86400000));
}

function dateKey(value: string | number | Date | null | undefined): string {
  if (!value) return "";
  if (typeof value === "string" && /^\d{4}-\d{2}-\d{2}/.test(value)) return value.slice(0, 10);
  const d = value instanceof Date ? value : new Date(value);
  if (isNaN(d.getTime())) return "";
  const y = d.getFullYear();
  const m = String(d.getMonth() + 1).padStart(2, "0");
  const day = String(d.getDate()).padStart(2, "0");
  return `${y}-${m}-${day}`;
}

function validPrice(value: unknown): number | null {
  const n = Number(value);
  return Number.isFinite(n) && n > 0 ? n : null;
}

function chartPreviousClose(result: Awaited<ReturnType<typeof getChart>>, cmp: number | null): number | null {
  const summary = result.summary as (typeof result.summary & { previous_close?: number | null }) | null;
  const fromSummary = validPrice(summary?.previous_close);
  if (fromSummary) return fromSummary;

  const changePct = Number(summary?.change_pct);
  if (cmp && Number.isFinite(changePct) && changePct > -99.9) {
    const derived = cmp / (1 + changePct / 100);
    const valid = validPrice(derived);
    if (valid) return valid;
  }

  const latestBarClose = validPrice(result.bars[result.bars.length - 1]?.close);
  const previousBarClose = validPrice(result.bars[result.bars.length - 2]?.close);
  if (!latestBarClose) return previousBarClose;
  if (!cmp) return previousBarClose ?? latestBarClose;

  const latestLooksLikeCmp = Math.abs(latestBarClose - cmp) / cmp <= 0.0025;
  return latestLooksLikeCmp ? (previousBarClose ?? latestBarClose) : latestBarClose;
}

function clampNumber(value: number, min: number, max: number) {
  return Math.min(max, Math.max(min, value));
}

function avgNumber(values: number[]) {
  return values.length ? values.reduce((sum, value) => sum + value, 0) / values.length : 0;
}

function qualityScoreForTrade(t: ClosedTrade) {
  const tags = t.tags || [];
  let score = 45;
  const setupText = `${t.setupType || ""} ${tags.join(" ")} ${t.remarks || ""}`.toLowerCase();
  if (MODEL_SETUP_WORDS.some((word) => setupText.includes(word))) score += 10;
  for (const tag of QUALITY_GOOD_TAGS) if (tags.includes(tag)) score += 7;
  for (const tag of QUALITY_BAD_TAGS) if (tags.includes(tag)) score -= 10;
  const stop = Number(t.stoploss);
  if (Number.isFinite(stop) && stop > 0 && t.entryPx > stop) {
    const posRiskPct = ((t.entryPx - stop) / t.entryPx) * 100;
    const accountRiskPct = t.equitySnapshot > 0 ? ((t.entryPx - stop) * t.qty / t.equitySnapshot) * 100 : 0;
    if (posRiskPct >= 2 && posRiskPct <= 4.5) score += 12;
    else if (posRiskPct <= 6) score += 6;
    else score -= 12;
    if (accountRiskPct <= 1) score += 10;
    else score -= 15;
  } else {
    score -= 18;
  }
  if (t.posSizePct > 0 && t.posSizePct <= 25) score += 4;
  if (t.perc < -6) score -= 10;
  return clampNumber(Math.round(score), 0, 100);
}

function gradeFromScore(score: number) {
  if (score >= 85) return "A+";
  if (score >= 75) return "A";
  if (score >= 65) return "B";
  if (score >= 50) return "C";
  return "D";
}

function calculateFIFO(trades: Trade[], startEquity: number, chargesConfig: ChargesConfig): FIFOResult {
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
        const grossPnl = (exitPx - entryPx) * matched;
        const product: Product = lot.trade.product === "intraday" ? "intraday" : "delivery";
        const breakdown = computeCharges({
          buyValue: entryPx * matched, sellValue: exitPx * matched, product, config: chargesConfig,
        });
        const pnl = grossPnl - breakdown.total;
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
          stoploss: Number(lot.trade.stoploss) || undefined,
          target: Number(lot.trade.target) || undefined,
          equitySnapshot: currentEquity, posSizePct,
          sellIndex: sellOrigIdx, buyIndices,
          product, grossPnl, charges: breakdown.total, breakdown,
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
    const openLots: OpenLot[] = [];
    let remarks = "", img: string | undefined, setupType: string | undefined;
    active.forEach(l => {
      const price = Number(l.trade.price) || 0;
      totalQty += l.remaining; totalInvested += l.remaining * price;
      buyIndices.push(l.origIdx);
      openLots.push({ qty: l.remaining, price, date: l.trade.date, buyIndex: l.origIdx });
      (l.trade.tags || []).forEach(t => { if (!tags.includes(t)) tags.push(t); });
      if (l.trade.remarks) remarks = l.trade.remarks;
      if (l.trade.img) img = l.trade.img;
      if (l.trade.setupType) setupType = l.trade.setupType;
    });
    openPositions.push({ symbol: sym, qty: totalQty, avgPx: totalQty > 0 ? totalInvested / totalQty : 0, totalInvested, buyIndices, tags, remarks, img, setupType, lots: openLots });
    openLotsDict[sym] = active.map(l => l.trade);
  });

  return { closedTrades, openPositions, currentEquity, openLotsDict };
}

// ─── Formatters ────────────────────────────────────────────────────────────────
function fmt(n: number, dec = 2) { return n.toLocaleString("en-IN", { minimumFractionDigits: dec, maximumFractionDigits: dec }); }
function fmtPnl(n: number) { return `${n >= 0 ? "+" : "−"}₹${fmt(Math.abs(n))}`; }
function fmtPerc(n: number) { return `${n >= 0 ? "+" : ""}${n.toFixed(2)}%`; }
function normSymbol(symbol: string) { return String(symbol || "").trim().toUpperCase().replace(/\.NS$/, ""); }
function finiteOrNull(value: number | null | undefined) {
  return typeof value === "number" && Number.isFinite(value) ? value : null;
}
function fmtMaybePct(value: number | null | undefined) {
  const n = finiteOrNull(value);
  return n === null ? "—" : fmtPerc(n);
}
function pctTone(value: number | null | undefined) {
  const n = finiteOrNull(value);
  if (n === null) return "";
  return n >= 0 ? "pos" : "neg";
}
function rankChangeText(value: number | null | undefined) {
  const n = finiteOrNull(value);
  if (n === null || n === 0) return "flat";
  return n > 0 ? `up ${Math.abs(n)}` : `down ${Math.abs(n)}`;
}

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
export function TradeJournalPanel({ market, addRequest, onAddRequestHandled, onOpenSymbolChart, groupsData }: TradeJournalPanelProps) {
  const [activeTab, setActiveTab] = useState(0);
  const [selectedEdge, setSelectedEdge] = useState<string | null>(null);
  useEffect(() => {
    if (!selectedEdge) return;
    const onKey = (e: KeyboardEvent) => {
      if (e.key === "Escape") setSelectedEdge(null);
    };
    window.addEventListener("keydown", onKey);
    return () => window.removeEventListener("keydown", onKey);
  }, [selectedEdge]);
  const [trades, setTrades] = useState<Trade[]>(() => lsGet<Trade[]>(LS_DATA, []));
  const [startEquity, setStartEquity] = useState<number>(() => lsGet<number>(LS_EQUITY, 100000));
  const [setups, setSetups] = useState<string[]>(() => {
    const reconciled = withDefaultSetups(lsGet<string[]>(LS_SETUPS, DEFAULT_SETUPS));
    lsSet(LS_SETUPS, reconciled); // purge retired setups from storage
    return reconciled;
  });
  const [openPosCats, setOpenPosCats] = useState<Record<string, OpenPosCat>>(() => lsGet(LS_POSITIONS, {}));
  const [posMeta, setPosMeta] = useState<Record<string, PosMeta>>(() => lsGet(LS_META, {}));
  const [chargesConfig, setChargesConfig] = useState<ChargesConfig>(() => ({ ...DEFAULT_CHARGES, ...lsGet<Partial<ChargesConfig>>(LS_CHARGES, {}) }));
  const [chargesPanelOpen, setChargesPanelOpen] = useState(false);
  const [breakdownTrade, setBreakdownTrade] = useState<ClosedTrade | null>(null);
  useEffect(() => {
    if (!breakdownTrade && !chargesPanelOpen) return;
    const onKey = (e: KeyboardEvent) => {
      if (e.key === "Escape") { setBreakdownTrade(null); setChargesPanelOpen(false); }
    };
    window.addEventListener("keydown", onKey);
    return () => window.removeEventListener("keydown", onKey);
  }, [breakdownTrade, chargesPanelOpen]);
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
  const [entryProduct, setEntryProduct] = useState<Product>("delivery");
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
  const [sizerProduct, setSizerProduct] = useState<Product>("delivery");

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
  const [modalEditSetupType, setModalEditSetupType] = useState("");
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
        const mergedSetups = withDefaultSetups(r.setups as string[]);
        setSetups(mergedSetups); lsSet(LS_SETUPS, mergedSetups);
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
    setScreenerSL(addRequest.suggestedStopLoss ? String(addRequest.suggestedStopLoss) : "");
    setScreenerDate(new Date().toISOString().split("T")[0]);
    setScreenerSetup(
      addRequest.setup && setups.includes(addRequest.setup)
        ? addRequest.setup
        : addRequest.setup || setups[0] || DEFAULT_SETUPS[0],
    );
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
  const fifo = useMemo(() => calculateFIFO(trades, startEquity, chargesConfig), [trades, startEquity, chargesConfig]);
  const { closedTrades, openPositions } = fifo;

  const updateChargesConfig = useCallback((next: ChargesConfig) => {
    setChargesConfig(next); lsSet(LS_CHARGES, next);
  }, []);

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

  // ── AI Coach ─────────────────────────────────────────────────────────────
  const [aiReview, setAiReview] = useState<AiJournalReview | null>(null);
  const [aiReviewLoading, setAiReviewLoading] = useState(false);

  // ── Edge analytics (million-dollar journal metrics) ─────────────────────
  const edge = useMemo(() => {
    const sortedClosed = [...closedTrades].sort((a, b) => new Date(a.exitDate).getTime() - new Date(b.exitDate).getTime());
    const wins = sortedClosed.filter((t) => t.pnl > 0);
    const losses = sortedClosed.filter((t) => t.pnl <= 0);
    const grossWin = wins.reduce((sum, t) => sum + t.pnl, 0);
    const grossLoss = Math.abs(losses.reduce((sum, t) => sum + t.pnl, 0));
    const avgWinPct = wins.length ? wins.reduce((sum, t) => sum + t.perc, 0) / wins.length : 0;
    const avgLossPct = losses.length ? losses.reduce((sum, t) => sum + t.perc, 0) / losses.length : 0;
    const winRate = sortedClosed.length ? wins.length / sortedClosed.length : 0;
    const expectancyPct = winRate * avgWinPct + (1 - winRate) * avgLossPct;
    // Max drawdown on the realized equity curve.
    let equity = startEquity, peak = startEquity, maxDd = 0;
    let streak = 0, bestStreak = 0, worstStreak = 0;
    for (const t of sortedClosed) {
      equity += t.pnl;
      peak = Math.max(peak, equity);
      maxDd = Math.max(maxDd, peak > 0 ? (peak - equity) / peak : 0);
      streak = t.pnl > 0 ? Math.max(1, streak + 1) : Math.min(-1, streak - 1);
      bestStreak = Math.max(bestStreak, streak);
      worstStreak = Math.min(worstStreak, streak);
    }
    // Rule discipline: the trader's own caps.
    const lossesOver4 = losses.filter((t) => t.perc < -4).length;
    const lossesOver6 = losses.filter((t) => t.perc < -6).length;
    // Monthly P&L
    const monthly = new Map<string, number>();
    for (const t of sortedClosed) {
      const key = String(t.exitDate).slice(0, 7);
      monthly.set(key, (monthly.get(key) ?? 0) + t.pnl);
    }
    const rTrades = sortedClosed
      .map((t) => {
        const stop = Number(t.stoploss);
        const riskPerShare = t.entryPx - stop;
        if (!Number.isFinite(stop) || stop <= 0 || riskPerShare <= 0) return null;
        return {
          ...t,
          r: (t.exitPx - t.entryPx) / riskPerShare,
          plannedRiskPct: (riskPerShare / t.entryPx) * 100,
        };
      })
      .filter((t): t is ClosedTrade & { r: number; plannedRiskPct: number } => Boolean(t));
    const avgR = rTrades.length ? rTrades.reduce((sum, t) => sum + t.r, 0) / rTrades.length : 0;
    const plannedRiskPct = rTrades.length ? rTrades.reduce((sum, t) => sum + t.plannedRiskPct, 0) / rTrades.length : 0;
    const twoRCount = rTrades.filter((t) => t.r >= 2).length;
    const minusOneRCount = rTrades.filter((t) => t.r <= -1).length;
    const plannedTradesPct = sortedClosed.length ? (rTrades.length / sortedClosed.length) * 100 : 0;
    const followedPlan = sortedClosed.filter((t) => t.tags?.includes("Followed Plan")).length;
    const brokePlan = sortedClosed.filter((t) => t.tags?.includes("Broke Plan")).length;
    const aPlusTrades = sortedClosed.filter((t) => {
      const tags = t.tags || [];
      const good = tags.includes("Followed Plan") || tags.includes("Perfect Entry") || tags.includes("Held Well");
      const bad = tags.some((tag) => ["FOMO", "Chased", "Broke Plan", "Averaged Down", "Emotional"].includes(tag));
      return good && !bad;
    });
    const habitCosts = new Map<string, { count: number; pnl: number }>();
    for (const t of sortedClosed) {
      for (const tag of t.tags || []) {
        if (!["FOMO", "Early Entry", "Late Entry", "Chased", "Sold Early", "Held Too Long", "Averaged Down", "Broke Plan", "Emotional"].includes(tag)) continue;
        const row = habitCosts.get(tag) ?? { count: 0, pnl: 0 };
        row.count += 1;
        row.pnl += t.pnl;
        habitCosts.set(tag, row);
      }
    }
    const costliestHabit = Array.from(habitCosts.entries()).sort((a, b) => a[1].pnl - b[1].pnl)[0] ?? null;
    const holdBuckets = [
      { label: "1d", min: 0, max: 1, trades: [] as ClosedTrade[] },
      { label: "2-5d", min: 2, max: 5, trades: [] as ClosedTrade[] },
      { label: "6-15d", min: 6, max: 15, trades: [] as ClosedTrade[] },
      { label: "16-30d", min: 16, max: 30, trades: [] as ClosedTrade[] },
      { label: "30d+", min: 31, max: Infinity, trades: [] as ClosedTrade[] },
    ];
    for (const t of sortedClosed) {
      const hold = Math.max(0, (getSafeTime(t.exitDate) - getSafeTime(t.entryDate)) / 86400000);
      const bucket = holdBuckets.find((b) => hold >= b.min && hold <= b.max);
      bucket?.trades.push(t);
    }
    const bestHoldBucket = holdBuckets
      .filter((b) => b.trades.length > 0)
      .map((b) => ({
        label: b.label,
        count: b.trades.length,
        pnl: b.trades.reduce((sum, t) => sum + t.pnl, 0),
        winRate: (b.trades.filter((t) => t.pnl > 0).length / b.trades.length) * 100,
      }))
      .sort((a, b) => b.pnl - a.pnl)[0] ?? null;
    const breadButterTrades = sortedClosed.filter((t) => {
      const text = `${t.setupType || ""} ${(t.tags || []).join(" ")} ${t.remarks || ""}`.toLowerCase();
      return text.includes("bread") || text.includes("pullback") || text.includes("stage 2") || text.includes("10 ema") || text.includes("21 ema");
    });
    const breadButterWins = breadButterTrades.filter((t) => t.pnl > 0).length;
    const breadButterPnl = breadButterTrades.reduce((sum, t) => sum + t.pnl, 0);
    const breadButterAvgPct = breadButterTrades.length
      ? breadButterTrades.reduce((sum, t) => sum + t.perc, 0) / breadButterTrades.length
      : 0;
    const best = sortedClosed.length ? sortedClosed.reduce((a, b) => (a.perc > b.perc ? a : b)) : null;
    const worst = sortedClosed.length ? sortedClosed.reduce((a, b) => (a.perc < b.perc ? a : b)) : null;
    return {
      profitFactor: grossLoss > 0 ? grossWin / grossLoss : grossWin > 0 ? Infinity : 0,
      expectancyPct,
      avgWinPct,
      avgLossPct,
      payoff: avgLossPct !== 0 ? Math.abs(avgWinPct / avgLossPct) : 0,
      maxDdPct: maxDd * 100,
      bestStreak,
      worstStreak: Math.abs(worstStreak),
      lossesOver4,
      lossesOver6,
      lossCount: losses.length,
      monthly: Array.from(monthly.entries()).sort((a, b) => b[0].localeCompare(a[0])).slice(0, 6),
      avgR,
      plannedRiskPct,
      twoRCount,
      minusOneRCount,
      rTradeCount: rTrades.length,
      plannedTradesPct,
      followedPlan,
      brokePlan,
      aPlusCount: aPlusTrades.length,
      aPlusWinRate: aPlusTrades.length ? (aPlusTrades.filter((t) => t.pnl > 0).length / aPlusTrades.length) * 100 : 0,
      costliestHabit,
      bestHoldBucket,
      breadButterCount: breadButterTrades.length,
      breadButterWinRate: breadButterTrades.length ? (breadButterWins / breadButterTrades.length) * 100 : 0,
      breadButterPnl,
      breadButterAvgPct,
      best,
      worst,
    };
  }, [closedTrades, startEquity]);

  const insightLab = useMemo(() => {
    const sortedClosed = [...closedTrades].sort((a, b) => getSafeTime(a.exitDate) - getSafeTime(b.exitDate));
    const qualityRows = sortedClosed.map((trade) => {
      const score = qualityScoreForTrade(trade);
      const stop = Number(trade.stoploss);
      const hasStop = Number.isFinite(stop) && stop > 0 && trade.entryPx > stop;
      const posRiskPct = hasStop ? ((trade.entryPx - stop) / trade.entryPx) * 100 : null;
      const accountRiskPct = hasStop && trade.equitySnapshot > 0 ? ((trade.entryPx - stop) * trade.qty / trade.equitySnapshot) * 100 : null;
      const rMultiple = hasStop ? (trade.exitPx - trade.entryPx) / (trade.entryPx - stop) : null;
      return {
        trade,
        score,
        grade: gradeFromScore(score),
        posRiskPct,
        accountRiskPct,
        rMultiple,
        holdDays: holdDays(trade.entryDate, trade.exitDate),
      };
    });
    const mistakeRows = MISTAKE_TAGS.map((tag) => {
      const tagged = sortedClosed.filter((trade) => (trade.tags || []).includes(tag));
      const pnl = tagged.reduce((sum, trade) => sum + trade.pnl, 0);
      const avgPct = avgNumber(tagged.map((trade) => trade.perc));
      const worst = tagged.length ? tagged.reduce((a, b) => (a.pnl < b.pnl ? a : b)) : null;
      return { tag, count: tagged.length, pnl, avgPct, worst };
    }).filter((row) => row.count > 0).sort((a, b) => a.pnl - b.pnl);
    const oneBigLeak = mistakeRows.find((row) => row.pnl < 0) ?? null;
    const aPlusTrades = qualityRows
      .filter((row) => row.score >= 80)
      .sort((a, b) => {
        const ar = a.rMultiple ?? -99;
        const br = b.rMultiple ?? -99;
        if (br !== ar) return br - ar;
        return b.trade.pnl - a.trade.pnl;
      })
      .slice(0, 6);
    const patternRows = new Map<string, { label: string; count: number; pnl: number; wins: number; example?: string }>();
    for (const trade of sortedClosed) {
      const labels = [
        trade.setupType ? `Setup: ${trade.setupType}` : "",
        ...(trade.tags || []).map((tag) => `Tag: ${tag}`),
      ].filter(Boolean);
      for (const label of labels) {
        const row = patternRows.get(label) ?? { label, count: 0, pnl: 0, wins: 0, example: trade.symbol };
        row.count += 1;
        row.pnl += trade.pnl;
        if (trade.pnl > 0) row.wins += 1;
        if (!row.example || trade.pnl < 0) row.example = trade.symbol;
        patternRows.set(label, row);
      }
    }
    const doNotTrade = Array.from(patternRows.values())
      .filter((row) => row.count >= 2 && row.pnl < 0)
      .map((row) => ({ ...row, winRate: row.count ? (row.wins / row.count) * 100 : 0 }))
      .sort((a, b) => a.pnl - b.pnl)
      .slice(0, 6);
    const sizingRows = qualityRows.filter((row) => row.posRiskPct !== null && row.accountRiskPct !== null);
    const correctSizing = sizingRows.filter((row) => (row.accountRiskPct ?? 99) <= 1 && (row.posRiskPct ?? 99) <= 6);
    const idealSizing = sizingRows.filter((row) => (row.accountRiskPct ?? 99) <= 1 && (row.posRiskPct ?? 99) >= 2 && (row.posRiskPct ?? 99) <= 4.5);
    const oversized = sizingRows
      .filter((row) => (row.accountRiskPct ?? 0) > 1 || (row.posRiskPct ?? 0) > 6)
      .sort((a, b) => (b.accountRiskPct ?? 0) - (a.accountRiskPct ?? 0))
      .slice(0, 5);
    const stopPlannedPct = sortedClosed.length ? (sizingRows.length / sortedClosed.length) * 100 : 0;
    const accountRiskOkPct = sizingRows.length ? (sizingRows.filter((row) => (row.accountRiskPct ?? 99) <= 1).length / sizingRows.length) * 100 : 0;
    const noMistakePct = sortedClosed.length
      ? (sortedClosed.filter((trade) => !(trade.tags || []).some((tag) => MISTAKE_TAGS.includes(tag))).length / sortedClosed.length) * 100
      : 0;
    const followedPlanPct = sortedClosed.length
      ? (sortedClosed.filter((trade) => (trade.tags || []).includes("Followed Plan")).length / sortedClosed.length) * 100
      : 0;
    const disciplineScore = Math.round(avgNumber([stopPlannedPct, accountRiskOkPct, noMistakePct, followedPlanPct]));
    const confidenceTags = ["High Conviction", "Medium Conviction", "Low Conviction", "A+ Setup", "B Setup", "C Setup"];
    const confidenceRows = confidenceTags.map((tag) => {
      const tagged = sortedClosed.filter((trade) => (trade.tags || []).includes(tag));
      return {
        tag,
        count: tagged.length,
        pnl: tagged.reduce((sum, trade) => sum + trade.pnl, 0),
        avgPct: avgNumber(tagged.map((trade) => trade.perc)),
      };
    }).filter((row) => row.count > 0);
    const openActions = openPositions.map((pos) => {
      const meta = posMeta[pos.symbol] || {};
      const cmp = meta.cmp || pos.avgPx;
      const sl = typeof meta.sl === "number" && meta.sl > 0 ? meta.sl : 0;
      const hasStop = sl > 0 && pos.avgPx > 0;
      const riskPerShare = hasStop ? pos.avgPx - sl : 0;
      const riskAmt = hasStop ? riskPerShare * pos.qty : 0;
      const riskPctEquity = startEquity > 0 ? Math.abs(riskAmt / startEquity) * 100 : 0;
      const pnlPct = pos.avgPx > 0 ? ((cmp - pos.avgPx) / pos.avgPx) * 100 : 0;
      const rMultiple = hasStop && riskPerShare > 0 ? (cmp - pos.avgPx) / riskPerShare : null;
      const weightPct = startEquity > 0 ? (pos.totalInvested / startEquity) * 100 : 0;
      let status: "Healthy" | "Watch" | "Act" = "Healthy";
      let action = "Hold while it respects your planned level.";
      if (!hasStop) {
        status = "Act";
        action = "Set a stop before judging the position.";
      } else if (riskPctEquity > 1) {
        status = "Act";
        action = "Reduce size or raise stop; account risk is above 1%.";
      } else if (pnlPct < -3 || (rMultiple !== null && rMultiple <= -0.7)) {
        status = "Watch";
        action = "Near the danger zone; do not widen the stop.";
      } else if (rMultiple !== null && rMultiple >= 2) {
        status = "Healthy";
        action = "Consider partial profit or trail stop to protect the win.";
      }
      return { symbol: pos.symbol, status, action, pnlPct, rMultiple, riskPctEquity, weightPct, hasStop };
    }).sort((a, b) => {
      const order = { Act: 0, Watch: 1, Healthy: 2 };
      return order[a.status] - order[b.status] || b.riskPctEquity - a.riskPctEquity;
    });
    const setupPlaybook = Object.entries(
      sortedClosed.reduce<Record<string, { trades: ClosedTrade[] }>>((acc, trade) => {
        const key = trade.setupType || "Unspecified";
        if (!acc[key]) acc[key] = { trades: [] };
        acc[key].trades.push(trade);
        return acc;
      }, {}),
    ).map(([setup, row]) => {
      const qRows = row.trades.map((trade) => qualityRows.find((q) => q.trade === trade)).filter(Boolean) as typeof qualityRows;
      const wins = row.trades.filter((trade) => trade.pnl > 0).length;
      const pnl = row.trades.reduce((sum, trade) => sum + trade.pnl, 0);
      const rValues = qRows.map((row) => row.rMultiple).filter((value): value is number => value !== null && Number.isFinite(value));
      const holds = row.trades.map((trade) => holdDays(trade.entryDate, trade.exitDate));
      return {
        setup,
        count: row.trades.length,
        pnl,
        winRate: row.trades.length ? (wins / row.trades.length) * 100 : 0,
        avgR: avgNumber(rValues),
        avgHold: avgNumber(holds),
        avgQuality: avgNumber(qRows.map((row) => row.score)),
      };
    }).sort((a, b) => b.pnl - a.pnl);
    const bestHold = edge.bestHoldBucket
      ? `Best results have come from ${edge.bestHoldBucket.label} holds (${fmtPnl(edge.bestHoldBucket.pnl)}).`
      : "Not enough closed trades to identify a hold-time edge.";
    const thesisLogged = sortedClosed.filter((trade) => (trade.remarks || "").trim().length >= 20).length;
    const thesisQualityPct = sortedClosed.length ? (thesisLogged / sortedClosed.length) * 100 : 0;
    const monthlyRows = edge.monthly.map(([month, pnl]) => ({ month, pnl }));
    const positiveMonths = monthlyRows.filter((row) => row.pnl > 0).length;
    const reportGrade = disciplineScore >= 85 && edge.expectancyPct > 0 ? "A" : disciplineScore >= 70 && edge.expectancyPct >= 0 ? "B" : disciplineScore >= 55 ? "C" : "D";
    const lossHoldAvg = avgNumber(sortedClosed.filter((trade) => trade.pnl <= 0).map((trade) => holdDays(trade.entryDate, trade.exitDate)));
    return {
      qualityRows,
      mistakeRows,
      oneBigLeak,
      aPlusTrades,
      doNotTrade,
      sizingRows,
      correctSizingPct: sizingRows.length ? (correctSizing.length / sizingRows.length) * 100 : 0,
      idealSizingPct: sizingRows.length ? (idealSizing.length / sizingRows.length) * 100 : 0,
      oversized,
      stopPlannedPct,
      accountRiskOkPct,
      noMistakePct,
      followedPlanPct,
      disciplineScore,
      confidenceRows,
      openActions,
      setupPlaybook,
      bestHold,
      loserCut: lossHoldAvg > 0 ? `Average loser is held ${lossHoldAvg.toFixed(1)} days; use that as the review deadline for weak trades.` : "Log more losses to define your cut window.",
      thesisQualityPct,
      reportGrade,
      positiveMonths,
      monthCount: monthlyRows.length,
    };
  }, [closedTrades, edge.bestHoldBucket, edge.expectancyPct, edge.monthly, openPositions, posMeta, startEquity]);

  const runAiCoach = () => {
    if (aiReviewLoading) return;
    setAiReviewLoading(true);
    setAiReview(null);
    runAiJournalReview(
      {
        starting_equity: startEquity,
        closed_trades: closedTrades.slice(-60).reverse(),
        open_positions: openPositions,
        analytics_summary: {
          expectancy_pct: Number(edge.expectancyPct.toFixed(2)),
          avg_r: Number(edge.avgR.toFixed(2)),
          planned_trades_pct: Number(edge.plannedTradesPct.toFixed(0)),
          discipline_score: insightLab.disciplineScore,
          report_grade: insightLab.reportGrade,
          one_big_leak: insightLab.oneBigLeak
            ? { tag: insightLab.oneBigLeak.tag, count: insightLab.oneBigLeak.count, pnl: Math.round(insightLab.oneBigLeak.pnl) }
            : null,
          mistake_costs: insightLab.mistakeRows.slice(0, 6).map((row) => ({ tag: row.tag, count: row.count, pnl: Math.round(row.pnl), avg_pct: Number(row.avgPct.toFixed(1)) })),
          do_not_trade: insightLab.doNotTrade.slice(0, 5).map((row) => ({ label: row.label, count: row.count, pnl: Math.round(row.pnl), win_rate: Number(row.winRate.toFixed(0)) })),
          sizing: {
            correct_pct: Number(insightLab.correctSizingPct.toFixed(0)),
            ideal_pct: Number(insightLab.idealSizingPct.toFixed(0)),
            oversized: insightLab.oversized.map((row) => ({
              symbol: row.trade.symbol,
              account_risk_pct: row.accountRiskPct !== null ? Number(row.accountRiskPct.toFixed(2)) : null,
              position_risk_pct: row.posRiskPct !== null ? Number(row.posRiskPct.toFixed(1)) : null,
            })),
          },
          quality: {
            avg_score: Number(avgNumber(insightLab.qualityRows.map((row) => row.score)).toFixed(0)),
            a_plus: insightLab.aPlusTrades.map((row) => ({ symbol: row.trade.symbol, setup: row.trade.setupType, score: row.score, r: row.rMultiple !== null ? Number(row.rMultiple.toFixed(2)) : null })),
          },
          open_actions: insightLab.openActions.map((row) => ({ symbol: row.symbol, status: row.status, risk_pct_equity: Number(row.riskPctEquity.toFixed(2)), action: row.action })),
        },
      },
      market ?? "india",
    )
      .then(setAiReview)
      .catch((error: unknown) =>
        setAiReview({ error: error instanceof Error ? error.message : "AI review failed." }),
      )
      .finally(() => setAiReviewLoading(false));
  };

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
        const price = validPrice(result.summary?.last_price) ?? validPrice(result.bars[result.bars.length - 1]?.close);
        const prevClose = chartPreviousClose(result, price);
        if (price) {
          if (!updated[pos.symbol]) updated[pos.symbol] = {};
          updated[pos.symbol].cmp = price;
          if (prevClose) updated[pos.symbol].prev_close = prevClose;
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
        const ticker = posMeta[pos.symbol]?.fetchTicker || pos.symbol;
        const result = await getChart(ticker, "1D", mkt);
        const price = validPrice(result.summary?.last_price) ?? validPrice(result.bars[result.bars.length - 1]?.close);
        const prevClose = chartPreviousClose(result, price);
        if (price) {
          if (!updated[pos.symbol]) updated[pos.symbol] = {};
          updated[pos.symbol].cmp = price;
          if (prevClose) updated[pos.symbol].prev_close = prevClose;
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

  // ── Weekly review (print-friendly report) ─────────────────────────────────
  function exportWeeklyReview() {
    const now = new Date();
    const weekAgo = new Date(now.getTime() - 7 * 24 * 3600 * 1000);
    const weekTrades = closedTrades.filter((t) => {
      const exit = new Date(t.exitDate);
      return !isNaN(exit.getTime()) && exit >= weekAgo && exit <= now;
    });
    const pnl = weekTrades.reduce((sum, t) => sum + t.pnl, 0);
    const wins = weekTrades.filter((t) => t.pnl > 0);
    const winRate = weekTrades.length ? Math.round((wins.length / weekTrades.length) * 100) : 0;
    const fmtINR = (v: number) => `Rs ${v.toLocaleString("en-IN", { maximumFractionDigits: 0 })}`;
    const bySetup = new Map<string, { count: number; pnl: number }>();
    for (const t of weekTrades) {
      const key = t.setupType || "(no setup)";
      const entry = bySetup.get(key) ?? { count: 0, pnl: 0 };
      entry.count += 1;
      entry.pnl += t.pnl;
      bySetup.set(key, entry);
    }
    const esc = (v: string) => v.replace(/&/g, "&amp;").replace(/</g, "&lt;").replace(/>/g, "&gt;");
    const rows = weekTrades
      .sort((a, b) => new Date(b.exitDate).getTime() - new Date(a.exitDate).getTime())
      .map(
        (t) => `<tr>
          <td>${esc(t.symbol)}</td><td>${esc(t.setupType || "—")}</td>
          <td>${t.exitDate?.slice(0, 10) || "—"}</td>
          <td class="num">${t.qty}</td>
          <td class="num">${t.entryPx.toFixed(2)}</td>
          <td class="num">${t.exitPx.toFixed(2)}</td>
          <td class="num ${t.pnl >= 0 ? "pos" : "neg"}">${fmtINR(t.pnl)}</td>
          <td class="num ${t.perc >= 0 ? "pos" : "neg"}">${t.perc >= 0 ? "+" : ""}${t.perc.toFixed(1)}%</td>
          <td>${esc((t.tags || []).join(", "))}</td>
        </tr>`,
      )
      .join("");
    const setupRows = Array.from(bySetup.entries())
      .sort((a, b) => b[1].pnl - a[1].pnl)
      .map(
        ([setup, agg]) =>
          `<tr><td>${esc(setup)}</td><td class="num">${agg.count}</td><td class="num ${agg.pnl >= 0 ? "pos" : "neg"}">${fmtINR(agg.pnl)}</td></tr>`,
      )
      .join("");
    const openRows = openPositions
      .map((p) => `<tr><td>${esc(p.symbol)}</td><td class="num">${p.qty}</td><td class="num">${p.avgPx.toFixed(2)}</td><td class="num">${fmtINR(p.totalInvested)}</td></tr>`)
      .join("");
    const html = `<!doctype html><html><head><meta charset="utf-8"><title>Weekly Review — ${now.toISOString().slice(0, 10)}</title>
      <style>
        body { font-family: -apple-system, "Segoe UI", sans-serif; margin: 32px; color: #111; }
        h1 { font-size: 20px; margin: 0 0 4px; } h2 { font-size: 14px; margin: 24px 0 8px; }
        .sub { color: #666; font-size: 12px; margin-bottom: 18px; }
        .kpis { display: flex; gap: 24px; margin: 14px 0 6px; }
        .kpi strong { display: block; font-size: 18px; } .kpi span { font-size: 11px; color: #666; }
        table { border-collapse: collapse; width: 100%; font-size: 12px; }
        th, td { padding: 6px 8px; border-bottom: 1px solid #e5e5e5; text-align: left; }
        th { font-size: 10px; text-transform: uppercase; letter-spacing: 0.05em; color: #666; }
        .num { text-align: right; font-variant-numeric: tabular-nums; }
        .pos { color: #047857; } .neg { color: #b91c1c; }
        @media print { body { margin: 12mm; } }
      </style></head><body>
      <h1>Weekly Trading Review</h1>
      <div class="sub">${weekAgo.toISOString().slice(0, 10)} → ${now.toISOString().slice(0, 10)} · generated by Mr. Malik Scanner</div>
      <div class="kpis">
        <div class="kpi"><strong class="${pnl >= 0 ? "pos" : "neg"}">${fmtINR(pnl)}</strong><span>Realized P&amp;L</span></div>
        <div class="kpi"><strong>${weekTrades.length}</strong><span>Closed trades</span></div>
        <div class="kpi"><strong>${winRate}%</strong><span>Win rate</span></div>
        <div class="kpi"><strong>${openPositions.length}</strong><span>Open positions</span></div>
      </div>
      <h2>Closed trades</h2>
      ${weekTrades.length ? `<table><thead><tr><th>Symbol</th><th>Setup</th><th>Exit</th><th class="num">Qty</th><th class="num">Entry</th><th class="num">Exit</th><th class="num">P&amp;L</th><th class="num">%</th><th>Tags</th></tr></thead><tbody>${rows}</tbody></table>` : "<p>No trades closed this week.</p>"}
      <h2>By setup</h2>
      ${setupRows ? `<table><thead><tr><th>Setup</th><th class="num">Trades</th><th class="num">P&amp;L</th></tr></thead><tbody>${setupRows}</tbody></table>` : "<p>—</p>"}
      <h2>Open positions</h2>
      ${openRows ? `<table><thead><tr><th>Symbol</th><th class="num">Qty</th><th class="num">Avg</th><th class="num">Invested</th></tr></thead><tbody>${openRows}</tbody></table>` : "<p>None.</p>"}
      <script>window.print();</script>
      </body></html>`;
    const win = window.open("", "_blank");
    if (win) {
      win.document.write(html);
      win.document.close();
    }
  }

  // ── Export / Import ───────────────────────────────────────────────────────
  function exportJSON() {
    const payload = { ...buildPayload(trades, startEquity, setups, openPosCats, posMeta), chargesConfig };
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
        const mergedSetups = withDefaultSetups(importedSetups);
        if (Array.isArray(importedSetups) && importedSetups.length > 0) { setSetups(mergedSetups); lsSet(LS_SETUPS, mergedSetups); }
        if (Object.keys(importedPositions).length > 0) { setOpenPosCats(importedPositions); lsSet(LS_POSITIONS, importedPositions); }
        if (Object.keys(importedMeta).length > 0) { setPosMeta(importedMeta); lsSet(LS_META, importedMeta); }
        const importedCharges = unwrap<Partial<ChargesConfig> | null>(data.chargesConfig, null);
        if (importedCharges && typeof importedCharges === "object") {
          updateChargesConfig({ ...DEFAULT_CHARGES, ...importedCharges });
        }
        // Push to backend after import
        syncToBackend(
          importedTrades.length ? importedTrades : trades,
          importedEquity > 0 ? importedEquity : startEquity,
          importedSetups.length ? mergedSetups : setups,
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
      vcp: { t: vcpT, depth: vcpDepth, vol: vcpVol }, product: entryProduct,
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
      stoploss: sl, target: 0, tags: [], remarks: "", vcp: {}, product: "delivery",
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
    setModalEditSetupType(origTrade.setupType || "");
    setModal({ type: "edit-closed", sellIndex, buyIndices });
  }

  function saveClosedEdits() {
    if (modal?.type !== "edit-closed") return;
    const { sellIndex, buyIndices } = modal;
    const customTags = modalEditCustomTags.split(",").map(s => s.trim()).filter(Boolean);
    const finalTags = [...modalEditTags, ...customTags];
    const newEntryPx = parseFloat(modalEditEntryPx), newExitPx = parseFloat(modalEditExitPx);
    const nextTrades = trades.map((t, i) => {
      if (buyIndices.includes(i)) return { ...t, tags: finalTags, remarks: modalEditRemarks, img: modalEditImg, setupType: modalEditSetupType, ...(!isNaN(newEntryPx) && newEntryPx > 0 ? { price: newEntryPx } : {}) };
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
  const todayKeyForOpenPnl = dateKey(new Date());
  const todayPnlParts = openPositions.reduce((acc, p) => {
    const meta = posMeta[p.symbol];
    const cmp = validPrice(meta?.cmp);
    if (!cmp) return acc;
    const lots = p.lots?.length ? p.lots : [{ qty: p.qty, price: p.avgPx, date: "", buyIndex: -1 }];
    for (const lot of lots) {
      const lotQty = Number(lot.qty) || 0;
      if (lotQty <= 0) continue;
      const boughtToday = dateKey(lot.date) === todayKeyForOpenPnl;
      const base = boughtToday ? validPrice(lot.price) : validPrice(meta?.prev_close);
      if (!base) continue;
      acc.pnl += (cmp - base) * lotQty;
      acc.base += base * lotQty;
      acc.coveredQty += lotQty;
      if (boughtToday) acc.intradayQty += lotQty;
    }
    return acc;
  }, { pnl: 0, base: 0, coveredQty: 0, intradayQty: 0 });
  const totalTodayPnl = todayPnlParts.pnl;
  const todayBaseValue = todayPnlParts.base;
  const hasTodayData = todayBaseValue > 0;
  const deployedPctEquity = startEquity > 0 ? (totalInvested / startEquity) * 100 : 0;
  const unrealPctDeployed = totalInvested > 0 ? (totalUnrealized / totalInvested) * 100 : 0;
  const riskPctEquity = startEquity > 0 ? (totalRisk / startEquity) * 100 : 0;
  const todayPctBase = hasTodayData ? (totalTodayPnl / todayBaseValue) * 100 : 0;
  const openGroupAnalysis = useMemo<OpenPositionGroupAnalysis[]>(() => {
    const groups = groupsData?.groups ?? [];
    const stocks = groupsData?.stocks ?? [];
    const stockBySymbol = new Map<string, IndustryGroupStockItem>();
    const groupById = new Map<string, IndustryGroupRankItem>();
    const stocksByGroup = new Map<string, IndustryGroupStockItem[]>();

    groups.forEach((group) => groupById.set(group.group_id, group));
    stocks.forEach((stock) => {
      stockBySymbol.set(normSymbol(stock.symbol), stock);
      const list = stocksByGroup.get(stock.final_group_id) ?? [];
      list.push(stock);
      stocksByGroup.set(stock.final_group_id, list);
    });
    stocksByGroup.forEach((list) => {
      list.sort((a, b) => {
        const aScore = (a.rs_rating ?? 0) * 4 + a.return_1m * 1.5 + a.return_6m + a.change_pct;
        const bScore = (b.rs_rating ?? 0) * 4 + b.return_1m * 1.5 + b.return_6m + b.change_pct;
        return bScore - aScore;
      });
    });

    return openPositions.map((position) => {
      const symbolKey = normSymbol(position.symbol);
      const stock = stockBySymbol.get(symbolKey);
      let group = stock ? groupById.get(stock.final_group_id) : undefined;
      if (!group) {
        group = groups.find((candidate) =>
          candidate.symbols?.some((symbol) => normSymbol(symbol) === symbolKey) ||
          candidate.top_constituents?.some((constituent) => normSymbol(constituent.symbol) === symbolKey)
        );
      }
      const groupStocks = stock
        ? stocksByGroup.get(stock.final_group_id) ?? []
        : group
          ? stocks.filter((item) => item.final_group_id === group?.group_id)
          : [];
      const stockRankIndex = groupStocks.findIndex((item) => normSymbol(item.symbol) === symbolKey);
      const topConstituentRank = group?.top_constituents?.findIndex((item) => normSymbol(item.symbol) === symbolKey) ?? -1;
      const stockRank = stockRankIndex >= 0 ? stockRankIndex + 1 : topConstituentRank >= 0 ? topConstituentRank + 1 : null;
      const stockCount = group?.stock_count || groupStocks.length || 0;
      const groupRank = finiteOrNull(group?.rank);
      const groupReturn1m = finiteOrNull(group?.return_1m);
      const groupReturn6m = finiteOrNull(group?.return_6m);
      const exposurePct = totalInvested > 0 ? (position.totalInvested / totalInvested) * 100 : 0;

      let verdict: GroupVerdict = "unknown";
      let note = "Group data unavailable; judge only by chart and risk until the group feed loads.";
      if (group) {
        const groupIsLeader = groupRank !== null && groupRank <= 20;
        const groupIsTradable = groupRank !== null && groupRank <= 40;
        const stockIsLeader = stockRank !== null && stockRank <= Math.max(5, Math.ceil(stockCount * 0.2));
        const groupMomentumOk = (groupReturn1m ?? 0) >= 0 && (groupReturn6m ?? 0) >= 0;
        const groupWeak = (groupRank !== null && groupRank > 60) || (groupReturn1m !== null && groupReturn1m < 0) || (groupReturn6m !== null && groupReturn6m < 0);
        if (groupIsLeader && stockIsLeader && groupMomentumOk) {
          verdict = "leader";
          note = "Leadership match: strong group plus a top-ranked stock inside it.";
        } else if (groupWeak) {
          verdict = "watch";
          note = "Group tailwind is weak; avoid adding unless price action is clearly exceptional.";
        } else if (groupIsTradable && groupMomentumOk) {
          verdict = "constructive";
          note = stockIsLeader ? "Good group tailwind; stock is near the top of its group." : "Group is fine, but stock is not one of the clear leaders.";
        } else {
          verdict = "watch";
          note = "Mixed group strength; keep size controlled and let price prove itself.";
        }
      }

      return {
        symbol: position.symbol,
        groupName: group?.group_name || stock?.final_group_name || "Unknown group",
        parentSector: group?.parent_sector || stock?.sector || "Unknown sector",
        groupRank,
        groupRankChange1w: finiteOrNull(group?.rank_change_1w),
        groupRankChange1m: finiteOrNull(group?.rank_change_1m),
        groupRankChange3m: finiteOrNull(group?.rank_change_3m),
        groupReturn1w: finiteOrNull(group?.return_1w),
        groupReturn1m,
        groupReturn6m,
        stockRank,
        stockCount,
        stockReturn1w: finiteOrNull(stock?.return_1w),
        stockReturn1m: finiteOrNull(stock?.return_1m),
        stockReturn6m: finiteOrNull(stock?.return_6m),
        rsRating: finiteOrNull(stock?.rs_rating),
        exposurePct,
        verdict,
        note,
      };
    });
  }, [groupsData, openPositions, totalInvested]);
  const openGroupBySymbol = useMemo(() => {
    const map = new Map<string, OpenPositionGroupAnalysis>();
    openGroupAnalysis.forEach((row) => map.set(normSymbol(row.symbol), row));
    return map;
  }, [openGroupAnalysis]);
  const openPositionAi = useMemo(() => {
    if (!openGroupAnalysis.length) return null;
    const knownRows = openGroupAnalysis.filter((row) => row.verdict !== "unknown");
    const leaders = openGroupAnalysis.filter((row) => row.verdict === "leader");
    const watch = openGroupAnalysis.filter((row) => row.verdict === "watch");
    const unknown = openGroupAnalysis.filter((row) => row.verdict === "unknown");
    const exposureByGroup = new Map<string, number>();
    knownRows.forEach((row) => exposureByGroup.set(row.groupName, (exposureByGroup.get(row.groupName) ?? 0) + row.exposurePct));
    const topGroupExposure = Array.from(exposureByGroup.entries()).sort((a, b) => b[1] - a[1])[0] ?? null;
    const rankedRows = knownRows.filter((row) => row.groupRank !== null);
    const avgGroupRank = rankedRows.length
      ? rankedRows.reduce((sum, row) => sum + (row.groupRank ?? 0), 0) / rankedRows.length
      : null;
    const concentrationWarning = topGroupExposure && topGroupExposure[1] >= 35;
    const headline = leaders.length >= Math.max(1, Math.ceil(openGroupAnalysis.length * 0.4))
      ? "Open book is leaning into leadership groups."
      : watch.length > leaders.length
        ? "Open book needs a stricter leadership check."
        : "Open book is mixed; let group strength decide adds.";
    const strengths = [
      leaders.length ? `${leaders.length} position${leaders.length === 1 ? "" : "s"} sit in leadership-quality groups.` : "No open position currently qualifies as a clear group leader.",
      avgGroupRank !== null && Number.isFinite(avgGroupRank) ? `Average known group rank is ${avgGroupRank.toFixed(1)}.` : "Group rank coverage is not available for enough positions yet.",
      topGroupExposure ? `Largest group exposure is ${topGroupExposure[0]} at ${topGroupExposure[1].toFixed(1)}% of deployed capital.` : "Group concentration cannot be judged yet.",
    ];
    const risks = [
      watch.length ? `${watch.length} position${watch.length === 1 ? "" : "s"} need watch-list treatment because group strength is mixed or weak.` : "No obvious weak-group positions from the current group feed.",
      concentrationWarning ? `Concentration is high in ${topGroupExposure?.[0]}; a group reversal can hit the book quickly.` : "No single group is above the 35% deployed-capital warning line.",
      unknown.length ? `${unknown.length} position${unknown.length === 1 ? "" : "s"} could not be mapped to a group.` : "Every open position was mapped to a group.",
    ];
    const actions = [
      "Add only when the stock is top-ranked inside its group and the group has positive 1M and 6M strength.",
      "Do not average up in watch-rated groups; demand clean price action and a tight stop.",
      concentrationWarning ? "Before the next entry, prefer a different leading group to reduce correlated risk." : "Keep the next add focused on the best group/stock combination, not just the best chart.",
    ];
    return { headline, strengths, risks, actions };
  }, [openGroupAnalysis]);
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
    const groupRow = openGroupBySymbol.get(normSymbol(p.symbol));
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

    // Position size as a fraction of starting equity — i.e. how much of the
    // total portfolio is parked in this name. Shown inline next to the
    // invested amount because that's the most natural place for it.
    const positionSizePct = startEquity > 0 ? (p.totalInvested / startEquity) * 100 : 0;

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

    // Live estimate of exit/round-trip charges and net P&L if closed at CMP.
    const posProduct: Product = fifo.openLotsDict[p.symbol]?.[0]?.product === "intraday" ? "intraday" : "delivery";
    const liveCharges = hasLive
      ? computeCharges({ buyValue: p.totalInvested, sellValue: cmp * totalQty, product: posProduct, config: chargesConfig })
      : null;
    const netUPnl = liveCharges ? uPnl - liveCharges.total : uPnl;

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
        {groupRow && (
          <div className={`tj-kcard-group tj-kcard-group-${groupRow.verdict}`}>
            <span className="tj-kcard-group-main" title={groupRow.groupName}>{groupRow.groupName}</span>
            <span>
              {groupRow.groupRank !== null ? `G#${groupRow.groupRank}` : "G#—"}
              {groupRow.stockRank !== null ? ` · S#${groupRow.stockRank}/${groupRow.stockCount || "?"}` : ""}
            </span>
          </div>
        )}
        <div className="tj-kcard-metrics">
          <div className="tj-kcard-metric"><span className="tj-kcard-ml">Avg / Invested</span><span title="Weighted-avg entry · total invested (size as % of starting equity)">₹{fmt(avgEntry)} · ₹{fmt(p.totalInvested, 0)} <small>({positionSizePct.toFixed(1)}%)</small></span></div>
          {hasLive && <div className="tj-kcard-metric"><span className="tj-kcard-ml">CMP</span><span className="tj-kcard-cmp">₹{fmt(cmp)}</span></div>}
          {hasLive && <div className={`tj-kcard-metric tj-kcard-pnl ${uPnl >= 0 ? "pos" : "neg"}`}><span className="tj-kcard-ml">P&L</span><span>{fmtPnl(uPnl)} <small>({fmtPerc(uPerc)})</small></span></div>}
          {hasLive && liveCharges && (
            <div className={`tj-kcard-metric ${netUPnl >= 0 ? "pos" : "neg"}`}>
              <span className="tj-kcard-ml">Net <small>({posProduct === "intraday" ? "MIS" : "CNC"})</small></span>
              <span title={`After estimated round-trip charges ₹${fmt(liveCharges.total)} (brokerage + STT + exchange + SEBI + stamp + GST${posProduct === "delivery" ? " + DP" : ""}) if closed at CMP`}>
                {fmtPnl(netUPnl)} <small>(− ₹{fmt(liveCharges.total, 0)})</small>
              </span>
            </div>
          )}
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
          <button className="tj-btn secondary" onClick={() => setChargesPanelOpen(true)} title="Brokerage & charges (Dhan) — net P&L is shown after these costs">⚙ Charges</button>
          <button className="tj-btn secondary" onClick={exportWeeklyReview} title="Print-friendly report of the last 7 days — trades, win rate, setup breakdown, open positions">Weekly Review</button>
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
                  <tr><th>Symbol</th><th>Setup</th><th>Entry ₹</th><th>Exit ₹</th><th>Entry</th><th>Exit</th><th title="Net of brokerage + all statutory charges. Click a value for the breakdown.">Net P&L ₹</th><th>%</th><th>Size %</th><th>Tags</th><th></th></tr>
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
                      <td className={t.pnl >= 0 ? "pos fw" : "neg fw"}>
                        <button type="button" className="tj-pnl-link" onClick={() => setBreakdownTrade(t)} title={`Net of ₹${fmt(t.charges)} charges (${t.product === "intraday" ? "Intraday" : "Delivery"}). Click for breakdown.`}>
                          {fmtPnl(t.pnl)}
                        </button>
                      </td>
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
                  {hasTodayData
                    ? `${fmtPerc(todayPctBase)} on ${todayPnlParts.intradayQty > 0 ? "today-entry/prev-close base" : "prev close"}`
                    : "Sync prices to compute"}
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
          {openPositions.length > 0 && (
            <section className="tj-open-analysis">
              <div className="tj-open-analysis-head">
                <div>
                  <h3>Open Position Group Analysis</h3>
                  <p>Leadership check using group rank, weekly/monthly/six-month strength, stock rank inside group, and deployed exposure.</p>
                </div>
                <span className="tj-open-analysis-stamp">{groupsData?.as_of_date ? `Groups as of ${groupsData.as_of_date}` : "Waiting for group feed"}</span>
              </div>
              {openPositionAi && (
                <div className="tj-open-ai-card">
                  <div className="tj-open-ai-title">AI Open Book Read</div>
                  <strong>{openPositionAi.headline}</strong>
                  <div className="tj-open-ai-grid">
                    <div>
                      <span className="tj-open-ai-label pos">Strengths</span>
                      <ul>{openPositionAi.strengths.map((item) => <li key={item}>{item}</li>)}</ul>
                    </div>
                    <div>
                      <span className="tj-open-ai-label neg">Risks</span>
                      <ul>{openPositionAi.risks.map((item) => <li key={item}>{item}</li>)}</ul>
                    </div>
                    <div>
                      <span className="tj-open-ai-label">Next actions</span>
                      <ul>{openPositionAi.actions.map((item) => <li key={item}>{item}</li>)}</ul>
                    </div>
                  </div>
                </div>
              )}
              <div className="tj-open-group-table-wrap">
                <table className="tj-open-group-table">
                  <thead>
                    <tr>
                      <th>Stock</th>
                      <th>Group</th>
                      <th>Group Rank</th>
                      <th>Group 1W / 1M / 6M</th>
                      <th>Stock Rank</th>
                      <th>Stock 1W / 1M / 6M</th>
                      <th>Exposure</th>
                      <th>Read</th>
                    </tr>
                  </thead>
                  <tbody>
                    {openGroupAnalysis.map((row) => (
                      <tr key={row.symbol}>
                        <td>
                          <button
                            type="button"
                            className="tj-symbol-link tj-symbol-link-inline"
                            onClick={() => onOpenSymbolChart?.(row.symbol)}
                          >
                            {row.symbol}
                          </button>
                        </td>
                        <td>
                          <strong>{row.groupName}</strong>
                          <span>{row.parentSector}</span>
                        </td>
                        <td>
                          <strong>{row.groupRank !== null ? `#${row.groupRank}` : "—"}</strong>
                          <span>1W {rankChangeText(row.groupRankChange1w)} · 1M {rankChangeText(row.groupRankChange1m)} · 3M {rankChangeText(row.groupRankChange3m)}</span>
                        </td>
                        <td className="tj-open-return-cell">
                          <span className={pctTone(row.groupReturn1w)}>{fmtMaybePct(row.groupReturn1w)}</span>
                          <span className={pctTone(row.groupReturn1m)}>{fmtMaybePct(row.groupReturn1m)}</span>
                          <span className={pctTone(row.groupReturn6m)}>{fmtMaybePct(row.groupReturn6m)}</span>
                        </td>
                        <td>
                          <strong>{row.stockRank !== null ? `#${row.stockRank}` : "—"}{row.stockCount ? ` / ${row.stockCount}` : ""}</strong>
                          <span>{row.rsRating !== null ? `RS ${row.rsRating.toFixed(0)}` : "RS unavailable"}</span>
                        </td>
                        <td className="tj-open-return-cell">
                          <span className={pctTone(row.stockReturn1w)}>{fmtMaybePct(row.stockReturn1w)}</span>
                          <span className={pctTone(row.stockReturn1m)}>{fmtMaybePct(row.stockReturn1m)}</span>
                          <span className={pctTone(row.stockReturn6m)}>{fmtMaybePct(row.stockReturn6m)}</span>
                        </td>
                        <td>
                          <strong>{row.exposurePct.toFixed(1)}%</strong>
                          <span>of deployed</span>
                        </td>
                        <td>
                          <span className={`tj-open-verdict tj-open-verdict-${row.verdict}`}>
                            {row.verdict === "leader" ? "Leader" : row.verdict === "constructive" ? "Constructive" : row.verdict === "watch" ? "Watch" : "Unknown"}
                          </span>
                          <span>{row.note}</span>
                        </td>
                      </tr>
                    ))}
                  </tbody>
                </table>
              </div>
            </section>
          )}
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
                <div className="tj-form-field"><label>Account Risk %</label><input className="tj-input" type="number" step="0.1" value={calcRisk} onChange={e => setCalcRisk(e.target.value)} /></div>
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
                  <div className="tj-form-field"><label>Product</label>
                    <select className="tj-select" value={entryProduct} onChange={e => setEntryProduct(e.target.value as Product)} title="Delivery (CNC) or Intraday (MIS) — drives the charges used for net P&L">
                      <option value="delivery">Delivery (CNC)</option>
                      <option value="intraday">Intraday (MIS)</option>
                    </select>
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
          <div className="tj-card tj-ai-coach">
            <div className="tj-card-hdr tj-ai-coach-hdr">
              <span>✦ AI Coach</span>
              <button className="tj-btn" onClick={runAiCoach} disabled={aiReviewLoading}>
                {aiReviewLoading ? "Reviewing your trading…" : aiReview ? "Re-run review" : "Review my trading"}
              </button>
            </div>
            {aiReviewLoading ? (
              <div className="tj-empty">Reading every trade, your tags, and the live tape on your open positions…</div>
            ) : aiReview?.error ? (
              <div className="tj-ai-error">{aiReview.error}</div>
            ) : aiReview?.raw ? (
              <div className="tj-ai-raw">{aiReview.raw}</div>
            ) : aiReview ? (
              <div className="tj-ai-body">
                {aiReview.overall ? <p className="tj-ai-overall">{formatAiReviewText(aiReview.overall)}</p> : null}
                <div className="tj-ai-columns">
                  {aiReview.doing_right?.length ? (
                    <div>
                      <div className="tj-ai-col-title pos">What you're doing right</div>
                      <ul>{aiReview.doing_right.map((item, index) => <li key={index}>{formatAiReviewText(item)}</li>)}</ul>
                    </div>
                  ) : null}
                  {aiReview.doing_wrong?.length ? (
                    <div>
                      <div className="tj-ai-col-title neg">What's hurting you</div>
                      <ul>{aiReview.doing_wrong.map((item, index) => <li key={index}>{formatAiReviewText(item)}</li>)}</ul>
                    </div>
                  ) : null}
                  {aiReview.fixes?.length ? (
                    <div>
                      <div className="tj-ai-col-title">Fixes</div>
                      <ul>{aiReview.fixes.map((item, index) => <li key={index}>{formatAiReviewText(item)}</li>)}</ul>
                    </div>
                  ) : null}
                </div>
                {(aiReview.one_big_leak || aiReview.monthly_report_card) ? (
                  <div className="tj-ai-columns">
                    {aiReview.one_big_leak ? (
                      <div>
                        <div className="tj-ai-col-title neg">One big leak</div>
                        <p className="tj-ai-mini-card">{formatAiReviewText(aiReview.one_big_leak)}</p>
                      </div>
                    ) : null}
                    {aiReview.monthly_report_card ? (
                      <div>
                        <div className="tj-ai-col-title">Report card</div>
                        <p className="tj-ai-mini-card">{formatAiReviewText(aiReview.monthly_report_card)}</p>
                      </div>
                    ) : null}
                  </div>
                ) : null}
                {(aiReview.risk_review?.length || aiReview.setup_playbook?.length || aiReview.do_not_trade?.length || aiReview.next_week_rules?.length) ? (
                  <div className="tj-ai-columns">
                    {aiReview.risk_review?.length ? (
                      <div>
                        <div className="tj-ai-col-title neg">Risk review</div>
                        <ul>{aiReview.risk_review.map((item, index) => <li key={index}>{formatAiReviewText(item)}</li>)}</ul>
                      </div>
                    ) : null}
                    {aiReview.setup_playbook?.length ? (
                      <div>
                        <div className="tj-ai-col-title pos">Setup playbook</div>
                        <ul>{aiReview.setup_playbook.map((item, index) => <li key={index}>{formatAiReviewText(item)}</li>)}</ul>
                      </div>
                    ) : null}
                    {aiReview.do_not_trade?.length ? (
                      <div>
                        <div className="tj-ai-col-title neg">Do not trade</div>
                        <ul>{aiReview.do_not_trade.map((item, index) => <li key={index}>{formatAiReviewText(item)}</li>)}</ul>
                      </div>
                    ) : null}
                    {aiReview.next_week_rules?.length ? (
                      <div>
                        <div className="tj-ai-col-title">Next week rules</div>
                        <ul>{aiReview.next_week_rules.map((item, index) => <li key={index}>{formatAiReviewText(item)}</li>)}</ul>
                      </div>
                    ) : null}
                  </div>
                ) : null}
                {aiReview.open_positions?.length ? (
                  <div className="tj-ai-positions">
                    <div className="tj-ai-col-title">Open positions</div>
                    <table className="tj-table">
                      <thead><tr><th>Symbol</th><th>Status</th><th>Tape read</th><th>Action</th></tr></thead>
                      <tbody>
                        {aiReview.open_positions.map((position, index) => (
                          <tr key={`${formatAiReviewText(position.symbol) || "position"}-${index}`}>
                            <td><strong>{formatAiReviewText(position.symbol) || "—"}</strong></td>
                            <td>
                              <span className={`tj-ai-status ${String(position.status || "").toLowerCase()}`}>
                                {formatAiReviewText(position.status) || "—"}
                              </span>
                            </td>
                            <td>{formatAiReviewText(position.read) || "—"}</td>
                            <td>{formatAiReviewText(position.action) || "—"}</td>
                          </tr>
                        ))}
                      </tbody>
                    </table>
                  </div>
                ) : null}
                {aiReview.one_lesson ? (
                  <p className="tj-ai-lesson"><strong>This week's lesson:</strong> {formatAiReviewText(aiReview.one_lesson)}</p>
                ) : null}
              </div>
            ) : (
              <div className="tj-empty">
                Get a coach-grade critique: what's working, what's costing you money, and a live read on every open
                position (accumulation vs distribution, hold or act).
              </div>
            )}
          </div>
          <div className="tj-insights-top" style={{ marginBottom: 14 }}>
            <div className="tj-card" style={{ flex: 2 }}>
              <div className="tj-card-hdr">Edge Analytics <small className="tj-edge-hint">tap any metric to learn what it means &amp; how to improve it</small></div>
              <div className="tj-edge-grid">
                <button type="button" className={`tj-edge-item tj-edge-clickable${selectedEdge === "expectancy" ? " is-active" : ""}`} onClick={() => setSelectedEdge((c) => (c === "expectancy" ? null : "expectancy"))}><span>Expectancy / trade</span><strong className={edge.expectancyPct >= 0 ? "pos" : "neg"}>{edge.expectancyPct >= 0 ? "+" : ""}{edge.expectancyPct.toFixed(2)}%</strong></button>
                <button type="button" className={`tj-edge-item tj-edge-clickable${selectedEdge === "profit_factor" ? " is-active" : ""}`} onClick={() => setSelectedEdge((c) => (c === "profit_factor" ? null : "profit_factor"))}><span>Profit factor</span><strong className={edge.profitFactor >= 1.5 ? "pos" : edge.profitFactor >= 1 ? "" : "neg"}>{Number.isFinite(edge.profitFactor) ? edge.profitFactor.toFixed(2) : "∞"}</strong></button>
                <button type="button" className={`tj-edge-item tj-edge-clickable${selectedEdge === "payoff" ? " is-active" : ""}`} onClick={() => setSelectedEdge((c) => (c === "payoff" ? null : "payoff"))}><span>Payoff (avg win / avg loss)</span><strong>{edge.payoff.toFixed(2)}</strong></button>
                <button type="button" className={`tj-edge-item tj-edge-clickable${selectedEdge === "avg_win_loss" ? " is-active" : ""}`} onClick={() => setSelectedEdge((c) => (c === "avg_win_loss" ? null : "avg_win_loss"))}><span>Avg win / Avg loss</span><strong><em className="pos">+{edge.avgWinPct.toFixed(1)}%</em> / <em className="neg">{edge.avgLossPct.toFixed(1)}%</em></strong></button>
                <button type="button" className={`tj-edge-item tj-edge-clickable${selectedEdge === "max_dd" ? " is-active" : ""}`} onClick={() => setSelectedEdge((c) => (c === "max_dd" ? null : "max_dd"))}><span>Max drawdown (realized)</span><strong className="neg">−{edge.maxDdPct.toFixed(1)}%</strong></button>
                <button type="button" className={`tj-edge-item tj-edge-clickable${selectedEdge === "streak" ? " is-active" : ""}`} onClick={() => setSelectedEdge((c) => (c === "streak" ? null : "streak"))}><span>Best / worst streak</span><strong>{edge.bestStreak}W / {edge.worstStreak}L</strong></button>
                <button type="button" className={`tj-edge-item tj-edge-clickable${selectedEdge === "loss_breaches" ? " is-active" : ""}`} onClick={() => setSelectedEdge((c) => (c === "loss_breaches" ? null : "loss_breaches"))}><span>Position loss breaches (&gt;4% / &gt;6%)</span><strong className={edge.lossesOver4 ? "neg" : "pos"}>{edge.lossesOver4} / {edge.lossesOver6} of {edge.lossCount}</strong></button>
                <button type="button" className={`tj-edge-item tj-edge-clickable${selectedEdge === "best_worst" ? " is-active" : ""}`} onClick={() => setSelectedEdge((c) => (c === "best_worst" ? null : "best_worst"))}><span>Best / worst trade</span><strong>{edge.best ? `${edge.best.symbol} +${edge.best.perc.toFixed(0)}%` : "—"} / {edge.worst ? `${edge.worst.symbol} ${edge.worst.perc.toFixed(0)}%` : "—"}</strong></button>
                <button type="button" className={`tj-edge-item tj-edge-clickable${selectedEdge === "avg_r" ? " is-active" : ""}`} onClick={() => setSelectedEdge((c) => (c === "avg_r" ? null : "avg_r"))}><span>Avg R / trade</span><strong className={edge.avgR >= 0 ? "pos" : "neg"}>{edge.rTradeCount ? `${edge.avgR >= 0 ? "+" : ""}${edge.avgR.toFixed(2)}R` : "Add stops"}</strong></button>
                <button type="button" className={`tj-edge-item tj-edge-clickable${selectedEdge === "r_multiples" ? " is-active" : ""}`} onClick={() => setSelectedEdge((c) => (c === "r_multiples" ? null : "r_multiples"))}><span>2R wins / -1R losses</span><strong>{edge.twoRCount} / {edge.minusOneRCount}</strong></button>
                <button type="button" className={`tj-edge-item tj-edge-clickable${selectedEdge === "avg_planned_risk" ? " is-active" : ""}`} onClick={() => setSelectedEdge((c) => (c === "avg_planned_risk" ? null : "avg_planned_risk"))}><span>Avg planned risk</span><strong>{edge.rTradeCount ? `${edge.plannedRiskPct.toFixed(1)}%` : "—"}</strong></button>
                <button type="button" className={`tj-edge-item tj-edge-clickable${selectedEdge === "stop_plan_pct" ? " is-active" : ""}`} onClick={() => setSelectedEdge((c) => (c === "stop_plan_pct" ? null : "stop_plan_pct"))}><span>Trades with stop plan</span><strong className={edge.plannedTradesPct >= 80 ? "pos" : edge.plannedTradesPct >= 50 ? "" : "neg"}>{edge.plannedTradesPct.toFixed(0)}%</strong></button>
              </div>
              {selectedEdge && EDGE_GLOSSARY[selectedEdge]
                ? createPortal(
                    <div className="tj-edge-modal-backdrop" onClick={() => setSelectedEdge(null)}>
                      <div className="tj-edge-modal" role="dialog" aria-modal="true" onClick={(e) => e.stopPropagation()}>
                        <div className="tj-edge-modal-hdr">
                          <div>
                            <small className="tj-edge-modal-eyebrow">Edge Analytics</small>
                            <strong>{EDGE_GLOSSARY[selectedEdge].title}</strong>
                          </div>
                          <button type="button" className="tj-edge-modal-close" onClick={() => setSelectedEdge(null)} aria-label="Close">×</button>
                        </div>
                        <div className="tj-edge-modal-body">
                          <section><span className="tj-edge-explain-tag">What it is</span><p>{EDGE_GLOSSARY[selectedEdge].what}</p></section>
                          <section><span className="tj-edge-explain-tag">How to improve it</span><p>{EDGE_GLOSSARY[selectedEdge].improve}</p></section>
                          <section><span className="tj-edge-explain-tag pos">What it does to your trading</span><p>{EDGE_GLOSSARY[selectedEdge].impact}</p></section>
                        </div>
                      </div>
                    </div>,
                    document.body,
                  )
                : null}
            </div>
            <div className="tj-card">
              <div className="tj-card-hdr">Monthly P&L</div>
              {edge.monthly.length === 0 ? <div className="tj-empty">No data yet</div> : (
                edge.monthly.map(([month, pnl]) => (
                  <div key={month} className={`tj-metric-row ${pnl >= 0 ? "pos" : "neg"}`}>
                    <span>{month}</span><strong>{fmtPnl(pnl)}</strong>
                  </div>
                ))
              )}
            </div>
          </div>
          <div className="tj-insight-command">
            <div className="tj-card tj-command-card">
              <div className="tj-card-hdr">One Big Leak</div>
              {insightLab.oneBigLeak ? (
                <>
                  <div className="tj-command-value neg">{insightLab.oneBigLeak.tag}</div>
                  <div className="tj-command-sub">
                    {insightLab.oneBigLeak.count} trades · {fmtPnl(insightLab.oneBigLeak.pnl)} · avg {insightLab.oneBigLeak.avgPct.toFixed(1)}%
                  </div>
                  <div className="tj-command-note">
                    Next 10 trades: block or halve size on this behavior until the cost stops growing.
                  </div>
                </>
              ) : (
                <>
                  <div className="tj-command-value pos">No major leak tagged</div>
                  <div className="tj-command-sub">Keep tagging mistakes so the journal can find the next leak.</div>
                </>
              )}
            </div>
            <div className="tj-card tj-command-card">
              <div className="tj-card-hdr">Discipline Score</div>
              <div className={`tj-command-value ${insightLab.disciplineScore >= 75 ? "pos" : insightLab.disciplineScore < 55 ? "neg" : ""}`}>
                {insightLab.disciplineScore}/100
              </div>
              <div className="tj-mini-bars">
                <span>Stops {insightLab.stopPlannedPct.toFixed(0)}%</span>
                <span>Risk ok {insightLab.accountRiskOkPct.toFixed(0)}%</span>
                <span>No mistake {insightLab.noMistakePct.toFixed(0)}%</span>
                <span>Plan {insightLab.followedPlanPct.toFixed(0)}%</span>
              </div>
            </div>
            <div className="tj-card tj-command-card">
              <div className="tj-card-hdr">Position Sizing Grade</div>
              <div className={`tj-command-value ${insightLab.correctSizingPct >= 80 ? "pos" : insightLab.correctSizingPct < 60 ? "neg" : ""}`}>
                {insightLab.sizingRows.length ? `${insightLab.correctSizingPct.toFixed(0)}% correct` : "Add stops"}
              </div>
              <div className="tj-command-sub">
                Ideal 3-4% position risk: {insightLab.sizingRows.length ? `${insightLab.idealSizingPct.toFixed(0)}%` : "—"}
              </div>
            </div>
            <div className="tj-card tj-command-card">
              <div className="tj-card-hdr">Monthly Trader Grade</div>
              <div className={`tj-command-value ${insightLab.reportGrade === "A" ? "pos" : insightLab.reportGrade === "D" ? "neg" : ""}`}>
                {insightLab.reportGrade}
              </div>
              <div className="tj-command-sub">
                {insightLab.positiveMonths}/{insightLab.monthCount || 0} profitable months · thesis logged {insightLab.thesisQualityPct.toFixed(0)}%
              </div>
            </div>
          </div>

          <div className="tj-insight-grid-2">
            <div className="tj-card">
              <div className="tj-card-hdr">Mistake Cost Dashboard</div>
              {insightLab.mistakeRows.length === 0 ? <div className="tj-empty">No mistake tags yet</div> : (
                <table className="tj-table tj-compact-table">
                  <thead><tr><th>Mistake</th><th>Trades</th><th>Avg</th><th>Cost</th><th>Worst</th></tr></thead>
                  <tbody>
                    {insightLab.mistakeRows.slice(0, 8).map((row) => (
                      <tr key={row.tag}>
                        <td><span className="tj-chip sm">{row.tag}</span></td>
                        <td>{row.count}</td>
                        <td className={row.avgPct >= 0 ? "pos" : "neg"}>{row.avgPct.toFixed(1)}%</td>
                        <td className={row.pnl >= 0 ? "pos fw" : "neg fw"}>{fmtPnl(row.pnl)}</td>
                        <td>{row.worst?.symbol ?? "—"}</td>
                      </tr>
                    ))}
                  </tbody>
                </table>
              )}
            </div>
            <div className="tj-card">
              <div className="tj-card-hdr">Do Not Trade Detector</div>
              {insightLab.doNotTrade.length === 0 ? <div className="tj-empty">No repeated losing pattern yet</div> : (
                <div className="tj-rule-list">
                  {insightLab.doNotTrade.map((row) => (
                    <div key={row.label} className="tj-rule-row">
                      <strong>{row.label}</strong>
                      <span>{row.count} trades · WR {row.winRate.toFixed(0)}% · <em className="neg">{fmtPnl(row.pnl)}</em></span>
                      <small>Rule: skip this until 10 clean samples prove it works.</small>
                    </div>
                  ))}
                </div>
              )}
            </div>
          </div>

          <div className="tj-insight-grid-2">
            <div className="tj-card">
              <div className="tj-card-hdr">A+ Trade Replay</div>
              {insightLab.aPlusTrades.length === 0 ? <div className="tj-empty">No A+ process trades yet</div> : (
                <div className="tj-replay-list">
                  {insightLab.aPlusTrades.map((row) => (
                    <button key={`${row.trade.symbol}-${row.trade.exitDate}-${row.score}`} type="button" className="tj-replay-row" onClick={() => onOpenSymbolChart?.(row.trade.symbol)}>
                      <span><strong>{row.trade.symbol}</strong><small>{row.trade.setupType || "Unspecified"} · {row.holdDays}d hold</small></span>
                      <span className={row.trade.pnl >= 0 ? "pos" : "neg"}>{row.grade} · {row.rMultiple !== null ? `${row.rMultiple.toFixed(1)}R` : fmtPerc(row.trade.perc)}</span>
                    </button>
                  ))}
                </div>
              )}
            </div>
            <div className="tj-card">
              <div className="tj-card-hdr">Trade Quality Score</div>
              <div className="tj-quality-band">
                {["A+", "A", "B", "C", "D"].map((grade) => {
                  const count = insightLab.qualityRows.filter((row) => row.grade === grade).length;
                  return <div key={grade}><strong>{count}</strong><span>{grade}</span></div>;
                })}
              </div>
              <div className="tj-rule-list">
                {insightLab.qualityRows.slice().sort((a, b) => a.score - b.score).slice(0, 4).map((row) => (
                  <div key={`${row.trade.symbol}-${row.trade.exitDate}-${row.score}`} className="tj-rule-row">
                    <strong>{row.trade.symbol} · {row.grade} ({row.score})</strong>
                    <span>{row.posRiskPct !== null ? `${row.posRiskPct.toFixed(1)}% pos risk` : "no stop"} · {fmtPnl(row.trade.pnl)}</span>
                  </div>
                ))}
              </div>
            </div>
          </div>

          <div className="tj-card" style={{ marginBottom: 14 }}>
            <div className="tj-card-hdr">Open Position Action Board</div>
            {insightLab.openActions.length === 0 ? <div className="tj-empty">No open positions</div> : (
              <table className="tj-table tj-compact-table">
                <thead><tr><th>Symbol</th><th>Status</th><th>P&L</th><th>R</th><th>Risk</th><th>Action</th></tr></thead>
                <tbody>
                  {insightLab.openActions.map((row) => (
                    <tr key={row.symbol}>
                      <td><button type="button" className="tj-symbol-link" onClick={() => onOpenSymbolChart?.(row.symbol)}>{row.symbol}</button></td>
                      <td><span className={`tj-ai-status ${row.status.toLowerCase()}`}>{row.status}</span></td>
                      <td className={row.pnlPct >= 0 ? "pos" : "neg"}>{row.pnlPct >= 0 ? "+" : ""}{row.pnlPct.toFixed(1)}%</td>
                      <td>{row.rMultiple !== null ? `${row.rMultiple >= 0 ? "+" : ""}${row.rMultiple.toFixed(1)}R` : "—"}</td>
                      <td className={row.riskPctEquity > 1 ? "neg fw" : ""}>{row.hasStop ? `${row.riskPctEquity.toFixed(2)}% eq` : "No SL"}</td>
                      <td>{row.action}</td>
                    </tr>
                  ))}
                </tbody>
              </table>
            )}
          </div>

          <div className="tj-insight-grid-2">
            <div className="tj-card">
              <div className="tj-card-hdr">Setup Playbook Analytics</div>
              {insightLab.setupPlaybook.length === 0 ? <div className="tj-empty">No setup samples yet</div> : (
                <table className="tj-table tj-compact-table">
                  <thead><tr><th>Setup</th><th>Trades</th><th>WR</th><th>Avg R</th><th>Hold</th><th>P&L</th></tr></thead>
                  <tbody>
                    {insightLab.setupPlaybook.slice(0, 8).map((row) => (
                      <tr key={row.setup}>
                        <td><span className="tj-chip sm">{row.setup}</span></td>
                        <td>{row.count}</td>
                        <td>{row.winRate.toFixed(0)}%</td>
                        <td>{row.avgR ? `${row.avgR >= 0 ? "+" : ""}${row.avgR.toFixed(2)}R` : "—"}</td>
                        <td>{row.avgHold.toFixed(1)}d</td>
                        <td className={row.pnl >= 0 ? "pos fw" : "neg fw"}>{fmtPnl(row.pnl)}</td>
                      </tr>
                    ))}
                  </tbody>
                </table>
              )}
            </div>
            <div className="tj-card">
              <div className="tj-card-hdr">Hold, Thesis & Confidence</div>
              <div className="tj-rule-list">
                <div className="tj-rule-row"><strong>Best time to hold</strong><span>{insightLab.bestHold}</span><small>{insightLab.loserCut}</small></div>
                <div className="tj-rule-row"><strong>Trade thesis logged</strong><span>{insightLab.thesisQualityPct.toFixed(0)}% of closed trades have meaningful remarks</span><small>At entry, write why buying, what proves wrong, where to add, where to sell.</small></div>
                <div className="tj-rule-row"><strong>Market Regime P&L</strong><span>Current journal does not store historical regime per trade yet.</span><small>Next upgrade: stamp regime at entry so this can split strong, choppy and weak tape performance.</small></div>
              </div>
              {insightLab.confidenceRows.length ? (
                <table className="tj-table tj-compact-table" style={{ marginTop: 10 }}>
                  <thead><tr><th>Confidence</th><th>Trades</th><th>Avg</th><th>P&L</th></tr></thead>
                  <tbody>
                    {insightLab.confidenceRows.map((row) => (
                      <tr key={row.tag}>
                        <td>{row.tag}</td><td>{row.count}</td><td className={row.avgPct >= 0 ? "pos" : "neg"}>{row.avgPct.toFixed(1)}%</td><td className={row.pnl >= 0 ? "pos fw" : "neg fw"}>{fmtPnl(row.pnl)}</td>
                      </tr>
                    ))}
                  </tbody>
                </table>
              ) : null}
            </div>
          </div>

          <div className="tj-card" style={{ marginBottom: 14 }}>
            <div className="tj-card-hdr">Trading Quality Dashboard</div>
            <div className="tj-edge-grid">
              <div className="tj-edge-item">
                <span>Bread & Butter model</span>
                <strong className={edge.breadButterPnl >= 0 ? "pos" : "neg"}>
                  {edge.breadButterCount ? `${edge.breadButterCount} trades · WR ${edge.breadButterWinRate.toFixed(0)}%` : "No tagged trades"}
                </strong>
              </div>
              <div className="tj-edge-item">
                <span>Model avg / net</span>
                <strong className={edge.breadButterPnl >= 0 ? "pos" : "neg"}>
                  {edge.breadButterCount ? `${edge.breadButterAvgPct >= 0 ? "+" : ""}${edge.breadButterAvgPct.toFixed(1)}% · ${fmtPnl(edge.breadButterPnl)}` : "—"}
                </strong>
              </div>
              <div className="tj-edge-item">
                <span>A+ execution</span>
                <strong>{edge.aPlusCount} trades · WR {edge.aPlusWinRate.toFixed(0)}%</strong>
              </div>
              <div className="tj-edge-item">
                <span>Plan discipline</span>
                <strong className={edge.brokePlan > edge.followedPlan ? "neg" : "pos"}>{edge.followedPlan} followed / {edge.brokePlan} broke</strong>
              </div>
              <div className="tj-edge-item">
                <span>Costliest habit</span>
                <strong className={edge.costliestHabit && edge.costliestHabit[1].pnl < 0 ? "neg" : ""}>
                  {edge.costliestHabit ? `${edge.costliestHabit[0]} · ${fmtPnl(edge.costliestHabit[1].pnl)}` : "No mistake tags"}
                </strong>
              </div>
              <div className="tj-edge-item">
                <span>Best hold zone</span>
                <strong>{edge.bestHoldBucket ? `${edge.bestHoldBucket.label} · ${fmtPnl(edge.bestHoldBucket.pnl)}` : "—"}</strong>
              </div>
            </div>
          </div>
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
              <div className="tj-form-field"><label>Account Risk (%)</label><input className="tj-input" type="number" step="0.1" value={sizerRiskPct} onChange={e => setSizerRiskPct(e.target.value)} /></div>
              <div className="tj-form-field"><label>Entry Price (₹)</label><input className="tj-input" type="number" step="any" value={sizerEntry} onChange={e => setSizerEntry(e.target.value)} /></div>
              <div className="tj-form-field"><label>Stop Loss / Position Risk (%)</label><input className="tj-input" type="number" step="0.1" value={sizerSLPct} onChange={e => setSizerSLPct(e.target.value)} /></div>
              <div className="tj-form-field"><label>Product</label>
                <select className="tj-select" value={sizerProduct} onChange={e => setSizerProduct(e.target.value as Product)}>
                  <option value="delivery">Delivery (CNC)</option>
                  <option value="intraday">Intraday (MIS)</option>
                </select>
              </div>
            </div>
            <div className="tj-sizer-results">
              <div className="tj-sizer-box"><div className="tj-sizer-label">Qty to Buy</div><div className="tj-sizer-val accent">{sizerResultQty}</div></div>
              <div className="tj-sizer-box"><div className="tj-sizer-label">SL Price</div><div className="tj-sizer-val neg">₹{fmt(sizerResultSL)}</div></div>
              <div className="tj-sizer-box"><div className="tj-sizer-label">Capital at Risk</div><div className="tj-sizer-val neg">₹{fmt(sizerResultRisk)}</div></div>
              <div className="tj-sizer-box"><div className="tj-sizer-label">Position Size</div><div className="tj-sizer-val">₹{fmt(sizerResultPos)}</div></div>
            </div>
            {sizerResultQty > 0 && sizerResultPos > 0 && (() => {
              const est = computeCharges({ buyValue: sizerResultPos, sellValue: sizerResultPos, product: sizerProduct, config: chargesConfig });
              const bePct = breakevenPct(sizerResultPos, est);
              return (
                <div className="tj-sizer-charges">
                  <div className="tj-sizer-charges-hdr">Estimated round-trip charges <span className="tj-prod-pill">{sizerProduct === "intraday" ? "Intraday" : "Delivery"}</span></div>
                  <div className="tj-sizer-results">
                    <div className="tj-sizer-box"><div className="tj-sizer-label">Round-trip charges</div><div className="tj-sizer-val neg">₹{fmt(est.total)}</div></div>
                    <div className="tj-sizer-box"><div className="tj-sizer-label">Breakeven move</div><div className="tj-sizer-val">{bePct.toFixed(2)}%</div></div>
                    <div className="tj-sizer-box"><div className="tj-sizer-label">Net at SL hit</div><div className="tj-sizer-val neg">{fmtPnl(-(sizerResultRisk + est.total))}</div></div>
                  </div>
                  <div className="tj-sizer-charges-note">Price must rise ~{bePct.toFixed(2)}% just to cover charges. Estimate assumes exit ≈ entry; edit rates under ⚙ Charges.</div>
                </div>
              );
            })()}
          </div>
        </div>
      )}

      {/* News Radar is now a full-screen modal triggered from the tab/button (see NewsModal mount below) */}

      {/* ── Modals ── (portaled to body so they always open centered on-screen,
            unaffected by the long page or any transformed ancestor) */}
      {modal && createPortal(
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
                  <div className="tj-form-field">
                    <label>Setup Type</label>
                    <select className="tj-input" value={modalEditSetupType} onChange={e => setModalEditSetupType(e.target.value)}>
                      <option value="">— None —</option>
                      {setups.map(s => <option key={s} value={s}>{s}</option>)}
                      {modalEditSetupType && !setups.includes(modalEditSetupType) ? <option value={modalEditSetupType}>{modalEditSetupType}</option> : null}
                    </select>
                  </div>
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
        </div>,
        document.body,
      )}
      <NewsModal
        isOpen={newsModalOpen}
        onClose={() => setNewsModalOpen(false)}
        title="News · Open Positions"
        symbols={openPositions.map(p => p.symbol)}
        market={market ?? "india"}
        accentColor="#06d6a0"
      />

      {/* ── Per-trade charges breakdown popup ── */}
      {breakdownTrade && createPortal(
        <div className="tj-edge-modal-backdrop" onClick={() => setBreakdownTrade(null)}>
          <div className="tj-edge-modal tj-charges-modal" role="dialog" aria-modal="true" onClick={(e) => e.stopPropagation()}>
            <div className="tj-edge-modal-hdr">
              <div>
                <small className="tj-edge-modal-eyebrow">Charges breakdown · {breakdownTrade.product === "intraday" ? "Intraday (MIS)" : "Delivery (CNC)"}</small>
                <strong>{breakdownTrade.symbol} · {Math.round(breakdownTrade.qty)} sh</strong>
              </div>
              <button type="button" className="tj-edge-modal-close" onClick={() => setBreakdownTrade(null)} aria-label="Close">×</button>
            </div>
            <div className="tj-edge-modal-body">
              <div className="tj-charges-row"><span>Buy {fmt(breakdownTrade.entryPx)} → Sell {fmt(breakdownTrade.exitPx)}</span><span></span></div>
              <div className="tj-charges-row tj-charges-gross"><span>Gross P&L</span><strong className={breakdownTrade.grossPnl >= 0 ? "pos" : "neg"}>{fmtPnl(breakdownTrade.grossPnl)}</strong></div>
              {(["brokerage", "stt", "exchange", "sebi", "stamp", "gst", "dp"] as const).map(k => (
                breakdownTrade.breakdown[k] > 0 ? (
                  <div key={k} className="tj-charges-row"><span>{CHARGE_LABELS[k]}</span><span className="neg">− ₹{fmt(breakdownTrade.breakdown[k])}</span></div>
                ) : null
              ))}
              <div className="tj-charges-row tj-charges-total"><span>Total charges</span><strong className="neg">− ₹{fmt(breakdownTrade.charges)}</strong></div>
              <div className="tj-charges-row tj-charges-net"><span>Net P&L</span><strong className={breakdownTrade.pnl >= 0 ? "pos" : "neg"}>{fmtPnl(breakdownTrade.pnl)}</strong></div>
              <div className="tj-charges-note">Charges per Dhan rates (editable under ⚙ Charges). DP charge approximated per closed delivery sell.</div>
            </div>
          </div>
        </div>,
        document.body,
      )}

      {/* ── Charges settings popover ── */}
      {chargesPanelOpen && createPortal(
        <div className="tj-edge-modal-backdrop" onClick={() => setChargesPanelOpen(false)}>
          <div className="tj-edge-modal tj-charges-modal" role="dialog" aria-modal="true" onClick={(e) => e.stopPropagation()}>
            <div className="tj-edge-modal-hdr">
              <div>
                <small className="tj-edge-modal-eyebrow">Brokerage & charges</small>
                <strong>Dhan / NSE equity rates</strong>
              </div>
              <button type="button" className="tj-edge-modal-close" onClick={() => setChargesPanelOpen(false)} aria-label="Close">×</button>
            </div>
            <div className="tj-edge-modal-body tj-charges-config">
              {([
                ["intradayBrokerageFlat", "Intraday brokerage cap (₹/order)"],
                ["intradayBrokeragePct", "Intraday brokerage (% /order)"],
                ["deliveryBrokerageFlat", "Delivery brokerage flat (₹/order)"],
                ["deliveryBrokeragePct", "Delivery brokerage (% /order)"],
                ["sttDeliveryPct", "STT delivery (% buy+sell)"],
                ["sttIntradayPct", "STT intraday (% sell)"],
                ["exchangePct", "Exchange txn (%)"],
                ["sebiPct", "SEBI fee (%)"],
                ["stampDeliveryPct", "Stamp duty delivery (% buy)"],
                ["stampIntradayPct", "Stamp duty intraday (% buy)"],
                ["gstPct", "GST (%)"],
                ["dpCharge", "DP charge (₹/delivery sell)"],
              ] as Array<[keyof ChargesConfig, string]>).map(([key, label]) => (
                <div key={key} className="tj-form-field">
                  <label>{label}</label>
                  <input
                    className="tj-input" type="number" step="any"
                    value={chargesConfig[key]}
                    onChange={e => updateChargesConfig({ ...chargesConfig, [key]: parseFloat(e.target.value) || 0 })}
                  />
                </div>
              ))}
              <div className="tj-charges-config-actions">
                <button type="button" className="tj-btn secondary" onClick={() => updateChargesConfig({ ...DEFAULT_CHARGES })}>Reset to Dhan defaults</button>
              </div>
              <div className="tj-charges-note">Net P&L across the journal — equity curve, win rate, Edge Analytics — is computed after these charges. Stored on this device.</div>
            </div>
          </div>
        </div>,
        document.body,
      )}
    </div>
  );
}
