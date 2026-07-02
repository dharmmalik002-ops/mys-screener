// Journal wisdom pack — pure analytics that turn the trade log into coaching.
// Everything here is computed from data the app already has (closed trades,
// the XP breadth history, daily chart bars, the scanner scorecard): no new
// backend endpoints, all functions total and null-safe so an empty journal
// renders empty states instead of crashing.
import type { XpBreadthPoint } from "./api";

export type ClosedTradeLike = {
  symbol: string;
  qty: number;
  entryPx: number;
  exitPx: number;
  entryDate: string;
  exitDate: string;
  pnl: number;
  perc: number;
  setupType?: string;
  tags?: string[];
  stoploss?: number;
};

const DAY_MS = 86_400_000;

function safeTime(date: string | undefined): number {
  const t = new Date(String(date ?? "")).getTime();
  return Number.isFinite(t) ? t : 0;
}

// ── 1. Regime join: which market weather was each trade entered in? ──────────

export type RegimeRow = {
  regime: string;
  color: string;
  trades: number;
  wins: number;
  winRate: number;
  pnl: number;
  avgPerc: number;
};

export type RegimeEdge = {
  rows: RegimeRow[];
  joined: number;
  unjoined: number;
  /** Realized P&L of trades ENTERED while the dial said "Avoid Longs". */
  againstDialPnl: number;
  againstDialTrades: number;
};

/** Latest XP point at or before the given date (history is ordered by date). */
export function regimeForDate(history: XpBreadthPoint[], date: string): XpBreadthPoint | null {
  if (!history?.length || !date) return null;
  const target = String(date).slice(0, 10);
  if (target < history[0].date) return null;
  let match: XpBreadthPoint | null = null;
  for (const point of history) {
    if (point.date > target) break;
    match = point;
  }
  return match;
}

export function computeRegimeEdge(closed: ClosedTradeLike[], history: XpBreadthPoint[] | null | undefined): RegimeEdge {
  const rows = new Map<string, RegimeRow>();
  let joined = 0;
  let unjoined = 0;
  let againstDialPnl = 0;
  let againstDialTrades = 0;
  for (const trade of closed ?? []) {
    const point = history ? regimeForDate(history, trade.entryDate) : null;
    if (!point || point.warmup) {
      unjoined += 1;
      continue;
    }
    joined += 1;
    const row = rows.get(point.regime) ?? {
      regime: point.regime,
      color: point.regime_color,
      trades: 0,
      wins: 0,
      winRate: 0,
      pnl: 0,
      avgPerc: 0,
    };
    row.trades += 1;
    if (trade.pnl > 0) row.wins += 1;
    row.pnl += trade.pnl;
    row.avgPerc += trade.perc;
    rows.set(point.regime, row);
    if (point.regime === "Avoid Longs") {
      againstDialPnl += trade.pnl;
      againstDialTrades += 1;
    }
  }
  const finished = Array.from(rows.values()).map((row) => ({
    ...row,
    winRate: row.trades ? (row.wins / row.trades) * 100 : 0,
    avgPerc: row.trades ? row.avgPerc / row.trades : 0,
  }));
  finished.sort((a, b) => b.trades - a.trades);
  return { rows: finished, joined, unjoined, againstDialPnl, againstDialTrades };
}

// ── 2. MAE / MFE: what did the trade do while you held it? ───────────────────

export type TradeExcursion = {
  /** Best unrealized gain during the hold, % from entry. */
  mfePct: number;
  /** Worst unrealized drawdown during the hold, % from entry (negative). */
  maePct: number;
  mfeR: number | null;
  maeR: number | null;
  realizedR: number | null;
};

export type BarLike = { time: number; high: number; low: number };

export function excursionKey(trade: ClosedTradeLike): string {
  return `${trade.symbol}|${trade.entryDate}|${trade.exitDate}|${trade.exitPx}`;
}

export function computeExcursion(trade: ClosedTradeLike, bars: BarLike[]): TradeExcursion | null {
  const start = safeTime(trade.entryDate) / 1000;
  const end = safeTime(trade.exitDate) / 1000 + DAY_MS / 1000; // include the exit day bar
  if (!start || !trade.entryPx || trade.entryPx <= 0) return null;
  const held = (bars ?? []).filter((bar) => bar.time >= start && bar.time <= end);
  if (!held.length) return null;
  let high = trade.entryPx;
  let low = trade.entryPx;
  for (const bar of held) {
    if (Number.isFinite(bar.high) && bar.high > high) high = bar.high;
    if (Number.isFinite(bar.low) && bar.low > 0 && bar.low < low) low = bar.low;
  }
  const mfePct = ((high - trade.entryPx) / trade.entryPx) * 100;
  const maePct = ((low - trade.entryPx) / trade.entryPx) * 100;
  const stop = Number(trade.stoploss);
  const riskPerShare = trade.entryPx - stop;
  const hasR = Number.isFinite(stop) && stop > 0 && riskPerShare > 0;
  return {
    mfePct,
    maePct,
    mfeR: hasR ? (high - trade.entryPx) / riskPerShare : null,
    maeR: hasR ? (low - trade.entryPx) / riskPerShare : null,
    realizedR: hasR ? (trade.exitPx - trade.entryPx) / riskPerShare : null,
  };
}

export type ExitQuality = {
  count: number;
  /** Avg realized R vs avg peak R on trades with a stop — the exit leak. */
  avgRealizedR: number | null;
  avgPeakR: number | null;
  /** % of the average peak move you actually banked (winners). */
  captureRatioPct: number | null;
  /** Winners that first sank below -0.7R before working: stop-too-tight signal. */
  deepDipWinnersPct: number | null;
  deepDipWinners: number;
  winnersWithR: number;
};

export function summarizeExitQuality(
  closed: ClosedTradeLike[],
  excursions: Record<string, TradeExcursion | null>,
): ExitQuality {
  const rows: Array<{ trade: ClosedTradeLike; exc: TradeExcursion }> = [];
  for (const trade of closed ?? []) {
    const exc = excursions[excursionKey(trade)];
    if (exc) rows.push({ trade, exc });
  }
  const withR = rows.filter((r) => r.exc.realizedR !== null && r.exc.mfeR !== null);
  const avgRealizedR = withR.length ? withR.reduce((s, r) => s + (r.exc.realizedR as number), 0) / withR.length : null;
  const avgPeakR = withR.length ? withR.reduce((s, r) => s + (r.exc.mfeR as number), 0) / withR.length : null;
  const winners = rows.filter((r) => r.trade.pnl > 0);
  const capture =
    winners.length && winners.some((r) => r.exc.mfePct > 0)
      ? (winners.reduce((s, r) => s + r.trade.perc, 0) / winners.length) /
        Math.max(0.0001, winners.reduce((s, r) => s + r.exc.mfePct, 0) / winners.length)
      : null;
  const winnersWithR = winners.filter((r) => r.exc.maeR !== null);
  const deepDip = winnersWithR.filter((r) => (r.exc.maeR as number) <= -0.7);
  return {
    count: rows.length,
    avgRealizedR,
    avgPeakR,
    captureRatioPct: capture === null ? null : Math.max(0, Math.min(150, capture * 100)),
    deepDipWinnersPct: winnersWithR.length ? (deepDip.length / winnersWithR.length) * 100 : null,
    deepDipWinners: deepDip.length,
    winnersWithR: winnersWithR.length,
  };
}

// ── 3. Tilt detector: what happens to you after losses? ─────────────────────

export type TiltStats = {
  baselineWinRate: number | null;
  baselineCount: number;
  afterLossWinRate: number | null;
  afterLossAvgPerc: number | null;
  afterLossCount: number;
  reentryCount: number;
  reentryPnl: number;
};

export function computeTiltStats(closed: ClosedTradeLike[]): TiltStats {
  const trades = [...(closed ?? [])].filter((t) => safeTime(t.entryDate) > 0);
  trades.sort((a, b) => safeTime(a.entryDate) - safeTime(b.entryDate));
  const byExit = [...trades].sort((a, b) => safeTime(a.exitDate) - safeTime(b.exitDate));

  const afterLoss: ClosedTradeLike[] = [];
  const baseline: ClosedTradeLike[] = [];
  for (const trade of trades) {
    const entry = safeTime(trade.entryDate);
    const prior = byExit.filter((t) => t !== trade && safeTime(t.exitDate) <= entry);
    const lastTwo = prior.slice(-2);
    if (lastTwo.length === 2 && lastTwo.every((t) => t.pnl < 0)) afterLoss.push(trade);
    else baseline.push(trade);
  }

  // Same-symbol re-entry within 7 calendar days of a losing exit.
  let reentryCount = 0;
  let reentryPnl = 0;
  for (const trade of trades) {
    const entry = safeTime(trade.entryDate);
    const priorLossSameSymbol = byExit.some(
      (t) =>
        t !== trade &&
        t.symbol === trade.symbol &&
        t.pnl < 0 &&
        safeTime(t.exitDate) <= entry &&
        entry - safeTime(t.exitDate) <= 7 * DAY_MS,
    );
    if (priorLossSameSymbol) {
      reentryCount += 1;
      reentryPnl += trade.pnl;
    }
  }

  const rate = (list: ClosedTradeLike[]) => (list.length ? (list.filter((t) => t.pnl > 0).length / list.length) * 100 : null);
  const avgPerc = (list: ClosedTradeLike[]) => (list.length ? list.reduce((s, t) => s + t.perc, 0) / list.length : null);
  return {
    baselineWinRate: rate(baseline),
    baselineCount: baseline.length,
    afterLossWinRate: rate(afterLoss),
    afterLossAvgPerc: avgPerc(afterLoss),
    afterLossCount: afterLoss.length,
    reentryCount,
    reentryPnl,
  };
}

// ── 4. Mentor card: lessons from the best, triggered by YOUR data ───────────

export type MentorContext = {
  soldEarlyCount: number;
  fomoCount: number;
  brokePlanCount: number;
  bigLossCount: number; // losses worse than -6%
  lossStreak: number; // current consecutive losing closed trades
  againstDialPnl: number;
  againstDialTrades: number;
  captureRatioPct: number | null;
  winRate: number | null;
  tradesLast30d: number;
};

export type MentorLesson = {
  quote: string;
  author: string;
  reason: string;
};

type LessonRule = {
  when: (ctx: MentorContext) => boolean;
  reason: (ctx: MentorContext) => string;
  quotes: Array<{ quote: string; author: string }>;
};

const LESSON_RULES: LessonRule[] = [
  {
    when: (c) => c.bigLossCount >= 2 || c.brokePlanCount >= 3,
    reason: (c) =>
      c.bigLossCount >= 2
        ? `shown because: ${c.bigLossCount} losses worse than -6% in your journal`
        : `shown because: ${c.brokePlanCount} trades tagged "Broke Plan"`,
    quotes: [
      { quote: "The elements of good trading are: cutting losses, cutting losses, and cutting losses.", author: "Ed Seykota" },
      { quote: "Losing a little is part of winning big. Take your small loss and go home — tomorrow the game reopens.", author: "Jesse Livermore" },
      { quote: "The whole secret to winning big in the stock market is not to be right all the time, but to lose the least amount possible when you're wrong.", author: "William O'Neil" },
    ],
  },
  {
    when: (c) => c.againstDialTrades >= 3 && c.againstDialPnl < 0,
    reason: (c) => `shown because: ${c.againstDialTrades} trades entered while XP said "Avoid Longs" cost you money`,
    quotes: [
      { quote: "When the market is against you, being out of the market IS a position — often the strongest one.", author: "Mark Minervini" },
      { quote: "Three out of four stocks follow the general market. Fight the tape and the odds fight you.", author: "William O'Neil" },
      { quote: "There is a time to go long, a time to go short, and a time to go fishing.", author: "Jesse Livermore" },
    ],
  },
  {
    when: (c) => c.soldEarlyCount >= 3 || (c.captureRatioPct !== null && c.captureRatioPct < 45),
    reason: (c) =>
      c.soldEarlyCount >= 3
        ? `shown because: ${c.soldEarlyCount} trades tagged "Sold Early"`
        : `shown because: you're banking only ~${Math.round(c.captureRatioPct ?? 0)}% of your winners' peak move`,
    quotes: [
      { quote: "It never was my thinking that made the big money for me. It always was my sitting.", author: "Jesse Livermore" },
      { quote: "The hardest part is not finding the big winner — it's sitting through the normal pullbacks without losing your position.", author: "Qullamaggie (Kristjan Kullamägi)" },
      { quote: "I made most of my money sitting on winners, not trading them.", author: "Nicolas Darvas" },
    ],
  },
  {
    when: (c) => c.fomoCount >= 3,
    reason: (c) => `shown because: ${c.fomoCount} trades tagged FOMO / Chased`,
    quotes: [
      { quote: "Buy at the pivot, not after the move. If you missed the bus, another one always comes.", author: "Dan Zanger" },
      { quote: "Wait for the setup. No setup, no trade — the market pays you for discipline, not activity.", author: "Mark Minervini" },
    ],
  },
  {
    when: (c) => c.lossStreak >= 3,
    reason: (c) => `shown because: you're on a ${c.lossStreak}-trade losing streak`,
    quotes: [
      { quote: "When you're losing, cut your size. Trade smaller and smaller until the market starts paying you again.", author: "Mark Minervini" },
      { quote: "After a losing streak, the goal isn't to make it back — it's to stop the bleeding and protect the mind.", author: "Mark Douglas" },
    ],
  },
  {
    when: (c) => c.tradesLast30d >= 25,
    reason: (c) => `shown because: ${c.tradesLast30d} trades in the last 30 days — activity is not edge`,
    quotes: [
      { quote: "Money is made by sitting, not trading.", author: "Jesse Livermore" },
      { quote: "The desire to constantly be in the market is the number one account killer.", author: "Stan Weinstein" },
    ],
  },
];

const DEFAULT_LESSONS: Array<{ quote: string; author: string }> = [
  { quote: "Become a specialist. Trade one or two setups until you know them better than anyone.", author: "Qullamaggie (Kristjan Kullamägi)" },
  { quote: "Risk comes first. Decide where you're wrong before you decide how much you can make.", author: "Mark Minervini" },
  { quote: "What seems too high and risky to the majority generally goes higher, and what seems low and cheap generally goes lower.", author: "William O'Neil" },
  { quote: "I never bought a stock at the low or sold one at the high in my life. I am satisfied to be along for most of the ride.", author: "Nicolas Darvas" },
  { quote: "The goal of a successful trader is to make the best trades. Money is secondary.", author: "Alexander Elder" },
  { quote: "Amateurs think about how much money they can make. Professionals think about how much money they could lose.", author: "Jack Schwager" },
];

export function pickMentorLesson(ctx: MentorContext, daySeed: number): MentorLesson {
  for (const rule of LESSON_RULES) {
    if (rule.when(ctx)) {
      const pick = rule.quotes[daySeed % rule.quotes.length];
      return { ...pick, reason: rule.reason(ctx) };
    }
  }
  const pick = DEFAULT_LESSONS[daySeed % DEFAULT_LESSONS.length];
  return { ...pick, reason: "daily lesson — no leaks detected this week, keep executing" };
}

export function buildMentorContext(
  closed: ClosedTradeLike[],
  regime: RegimeEdge,
  exitQuality: ExitQuality,
): MentorContext {
  const now = Date.now();
  const recent = (closed ?? []).filter((t) => now - safeTime(t.exitDate) <= 90 * DAY_MS);
  const tagCount = (tags: string[]) =>
    recent.filter((t) => (t.tags ?? []).some((tag) => tags.includes(tag))).length;
  const byExit = [...(closed ?? [])].sort((a, b) => safeTime(a.exitDate) - safeTime(b.exitDate));
  let lossStreak = 0;
  for (let i = byExit.length - 1; i >= 0; i -= 1) {
    if (byExit[i].pnl < 0) lossStreak += 1;
    else break;
  }
  const closedCount = closed?.length ?? 0;
  return {
    soldEarlyCount: tagCount(["Sold Early"]),
    fomoCount: tagCount(["FOMO", "Chased"]),
    brokePlanCount: tagCount(["Broke Plan"]),
    bigLossCount: recent.filter((t) => t.perc < -6).length,
    lossStreak,
    againstDialPnl: regime.againstDialPnl,
    againstDialTrades: regime.againstDialTrades,
    captureRatioPct: exitQuality.captureRatioPct,
    winRate: closedCount ? (closed.filter((t) => t.pnl > 0).length / closedCount) * 100 : null,
    tradesLast30d: (closed ?? []).filter((t) => now - safeTime(t.entryDate) <= 30 * DAY_MS).length,
  };
}
