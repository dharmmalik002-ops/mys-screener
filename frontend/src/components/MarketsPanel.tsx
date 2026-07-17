import { useEffect, useMemo, useState } from "react";
import { CartesianGrid, Line, LineChart, ResponsiveContainer, Tooltip, XAxis, YAxis, Legend } from "recharts";
import { ArrowDownRight, ArrowUpRight, Compass, Minus } from "lucide-react";

import {
  getChart,
  getMarketEnvironment,
  getScanCounts,
  type ChartBar,
  type MarketEnvironmentResponse,
  type MarketEnvDay,
  type XpBreadthScore,
} from "../lib/api";
import { IndexCandleChart } from "./IndexCandleChart";
import { Panel } from "./Panel";

import "./MarketsPanel.css";

// ---------------------------------------------------------------------------
// Market Outlook engine — the page's backbone.
// A transparent weight-of-evidence model: every input is a measurable signal
// with a stated weight and one-line logic. No black box, no prophecy — the
// verdict is exactly the sum of what's on the table.
// ---------------------------------------------------------------------------

// Smallcap 250 and Midcap 150 get full candlestick charts (the user's hunting
// ground); Nifty 50 is fetched ONLY as an outlook signal — no chart rendered.
const OUTLOOK_INDICES = [
  { symbol: "NIFTYSMLCAP250.NS", label: "Smallcap 250", chart: true },
  { symbol: "NIFTYMIDCAP150.NS", label: "Midcap 150", chart: true },
  { symbol: "^NSEI", label: "Nifty 50", chart: false },
] as const;

type IndexHealth = {
  symbol: string;
  label: string;
  last: number;
  ret20Pct: number | null;
  ret60Pct: number | null;
  distFrom52wHighPct: number | null;
  sma20: number | null;
  sma50: number | null;
  sma200: number | null;
  above50: boolean;
  above200: boolean;
  sma50Rising: boolean;
  state: string;
  stateScore: number; // -2 .. +2
  spark: number[]; // trailing closes for the sparkline
};

function simpleAvg(values: number[]): number | null {
  return values.length ? values.reduce((a, b) => a + b, 0) / values.length : null;
}

function computeIndexHealth(symbol: string, label: string, bars: ChartBar[]): IndexHealth | null {
  const closes = bars.map((b) => b.close).filter((c) => Number.isFinite(c) && c > 0);
  if (closes.length < 60) return null;
  const last = closes[closes.length - 1];
  const sma20 = simpleAvg(closes.slice(-20));
  const sma50 = simpleAvg(closes.slice(-50));
  const sma200 = closes.length >= 200 ? simpleAvg(closes.slice(-200)) : null;
  const sma50Prev = closes.length >= 60 ? simpleAvg(closes.slice(-60, -10)) : null;
  const ret = (n: number) => (closes.length > n ? ((last / closes[closes.length - 1 - n]) - 1) * 100 : null);
  const high52 = Math.max(...closes.slice(-252));
  const above50 = sma50 !== null && last > sma50;
  const above200 = sma200 === null ? above50 : last > sma200;
  const sma50Rising = sma50 !== null && sma50Prev !== null && sma50 > sma50Prev;

  let state = "Downtrend";
  let stateScore = -2;
  if (above50 && (sma200 === null || (sma50 !== null && sma50 > sma200))) {
    state = "Confirmed Uptrend";
    stateScore = 2;
  } else if (above50) {
    state = "Attempting Recovery";
    stateScore = 1;
  } else if (above200) {
    state = "Pullback / Basing";
    stateScore = -0.5;
  }

  return {
    symbol,
    label,
    last,
    ret20Pct: ret(20),
    ret60Pct: ret(60),
    distFrom52wHighPct: high52 > 0 ? ((high52 - last) / high52) * 100 : null,
    sma20,
    sma50,
    sma200,
    above50,
    above200,
    sma50Rising,
    state,
    stateScore,
    spark: closes.slice(-120),
  };
}

type OutlookSignal = {
  label: string;
  valueLabel: string;
  score: number; // -2 .. +2
  weight: number;
  logic: string;
};

type Outlook = {
  score: number; // -100 .. +100
  verdict: string;
  guidance: string;
  tone: "pos" | "neu" | "neg";
  signals: OutlookSignal[];
  reasons: string[];
  flips: string[];
};

const clamp2 = (v: number) => Math.max(-2, Math.min(2, v));

function buildOutlook(
  xp: XpBreadthScore | null,
  env: MarketEnvironmentResponse | null,
  indexHealth: Record<string, IndexHealth | null>,
): Outlook | null {
  const signals: OutlookSignal[] = [];

  if (xp) {
    const s = xp.xp_score;
    let score = s > 25 ? 2 : s >= 15 ? 1.2 : s >= 12 ? 0.4 : s >= 9.5 ? -0.6 : -2;
    const live = xp.history.filter((p) => !p.warmup);
    const back = live.length >= 6 ? live[live.length - 6].xp_score : null;
    const slope = back !== null ? s - back : 0;
    if (slope >= 1) score = clamp2(score + 0.4);
    else if (slope <= -1) score = clamp2(score - 0.4);
    signals.push({
      label: "XP breadth regime",
      valueLabel: `${s.toFixed(1)} · ${xp.regime}${back !== null ? ` · ${slope >= 0 ? "+" : ""}${slope.toFixed(1)} vs last wk` : ""}`,
      score,
      weight: 3,
      logic: "Breadth is the market's engine room — sustained smallcap runs only happen with the XP score in the swing-friendly bands and rising.",
    });
  }

  const small = indexHealth["NIFTYSMLCAP250.NS"];
  if (small) {
    let score = small.stateScore;
    if (small.sma50Rising) score = clamp2(score + 0.3);
    signals.push({
      label: "Smallcap 250 trend",
      valueLabel: `${small.state}${small.ret20Pct !== null ? ` · ${small.ret20Pct >= 0 ? "+" : ""}${small.ret20Pct.toFixed(1)}% / 20d` : ""}`,
      score,
      weight: 3,
      logic: "Your hunting ground. Small caps lead in both directions — their index above a rising 50-DMA is the single best backdrop for VCP/Power Base entries.",
    });
  }

  const nifty = indexHealth["^NSEI"];
  if (nifty) {
    signals.push({
      label: "Nifty 50 trend",
      valueLabel: nifty.state,
      score: nifty.stateScore * 0.75,
      weight: 1.5,
      logic: "The tide. Smallcap rallies against a falling Nifty have short lifespans; alignment lengthens every hold.",
    });
  }

  const posture = env?.posture;
  const hist = env?.history ?? [];
  if (posture?.above_sma200_pct != null) {
    const v = posture.above_sma200_pct;
    let score = v >= 60 ? 1.5 : v >= 45 ? 0.5 : v >= 35 ? -0.5 : -1.5;
    const first = hist.find((h) => h.above_sma200_pct != null)?.above_sma200_pct;
    if (first != null && hist.length >= 3) {
      if (v - first >= 3) score = clamp2(score + 0.5);
      else if (v - first <= -3) score = clamp2(score - 0.5);
    }
    signals.push({
      label: "% of stocks above 200-SMA",
      valueLabel: `${Math.round(v)}%`,
      score,
      weight: 2,
      logic: "The structural tide — how much of the market is in a long-term uptrend. Durable multi-week advances need a majority above.",
    });
  }
  if (posture?.above_ema21_pct != null) {
    const v = posture.above_ema21_pct;
    signals.push({
      label: "% of stocks above 21-EMA",
      valueLabel: `${Math.round(v)}%`,
      score: v >= 65 ? 1.5 : v >= 50 ? 0.5 : v >= 35 ? -0.5 : -1.5,
      weight: 1.5,
      logic: "The short-term thrust gauge. Swing entries work when the majority holds the fast average; below 35% the tape is in liquidation mode.",
    });
  }
  const held = env?.today?.structural?.held_pct;
  if (held != null) {
    signals.push({
      label: "Breakout follow-through",
      valueLabel: `${Math.round(held)}% still above pivot`,
      score: held >= 60 ? 1.5 : held >= 45 ? 0.5 : held >= 30 ? -0.5 : -1.5,
      weight: 2,
      logic: "The truth serum. If recent breakouts are holding their pivots, new entries get paid; if they fail, even perfect setups bleed.",
    });
  }
  if (posture) {
    const h = posture.new_52w_highs;
    const l = posture.new_52w_lows;
    signals.push({
      label: "New 52w highs vs lows",
      valueLabel: `${h} / ${l}`,
      score: h >= 3 * Math.max(1, l) && h >= 10 ? 1.5 : h > l ? 0.5 : l >= 3 * Math.max(1, h) && l >= 10 ? -2 : -1,
      weight: 1,
      logic: "Leadership check — bull phases mint new highs daily. An expansion of new lows is the earliest structural crack.",
    });
  }

  if (signals.length < 3) return null;

  const totalWeight = signals.reduce((a, s) => a + s.weight * 2, 0);
  const raw = signals.reduce((a, s) => a + s.score * s.weight, 0);
  const score = Math.round((raw / totalWeight) * 100);

  let verdict = "NEUTRAL / CHOPPY";
  let guidance = "Trade small, take profits fast, and let the tape prove itself before adding.";
  let tone: Outlook["tone"] = "neu";
  if (score >= 45) {
    verdict = "RISK-ON";
    guidance = "Breadth, trend and follow-through agree. Full position sizes on A setups are justified.";
    tone = "pos";
  } else if (score >= 15) {
    verdict = "CONSTRUCTIVE";
    guidance = "The tape is improving. Add selectively on your best setups with tight stops.";
    tone = "pos";
  } else if (score <= -45) {
    verdict = "RISK-OFF";
    guidance = "Stand aside. Cash is a position; protect capital until the evidence turns.";
    tone = "neg";
  } else if (score <= -15) {
    verdict = "DEFENSIVE";
    guidance = "Raise cash, cut laggards, and only touch A+ setups at reduced size.";
    tone = "neg";
  }

  const byImpact = [...signals].sort((a, b) => Math.abs(b.score * b.weight) - Math.abs(a.score * a.weight));
  const reasons = byImpact.slice(0, 3).map((s) => {
    const dir = s.score > 0.2 ? "supports" : s.score < -0.2 ? "works against" : "is neutral for";
    return `${s.label} (${s.valueLabel}) ${dir} the market right now.`;
  });

  const nearFlip = [...signals].sort((a, b) => Math.abs(a.score) - Math.abs(b.score)).slice(0, 2);
  const flips = nearFlip.map((s) =>
    s.score <= 0
      ? `${s.label} turning decisively up would upgrade this outlook.`
      : `${s.label} rolling over would downgrade this outlook.`,
  );

  return { score, verdict, guidance, tone, signals, reasons, flips };
}

// ---------------------------------------------------------------------------
// "Underneath the surface" — the full internals read. Not "the index is up":
// who is leading, whether breakouts are being paid, whether dips are bought,
// where money is rotating, and how many bases are loading for NEXT month.
// Every sentence is generated from live counted data.
// ---------------------------------------------------------------------------

type UnderneathRead = {
  paras: Array<{ title: string; text: string }>;
};

function buildUnderneath(
  env: MarketEnvironmentResponse | null,
  setupCounts: Record<string, number> | null,
  small: IndexHealth | null,
  xp: XpBreadthScore | null,
): UnderneathRead | null {
  if (!env) return null;
  const posture = env.posture;
  const ev = env.evidence;
  const paras: Array<{ title: string; text: string }> = [];

  // --- Participation: is the crowd moving with the index? ---
  if (posture) {
    const adv = posture.advances;
    const dec = posture.declines;
    const advPct = adv + dec > 0 ? Math.round((adv / (adv + dec)) * 100) : null;
    const a21 = posture.above_ema21_pct;
    const a200 = posture.above_sma200_pct;
    let divergence = "";
    if (a21 != null && a200 != null) {
      if (a200 >= 55 && a21 < 45) {
        divergence = " That gap — structure healthy but the short-term average lost — is a correction inside an uptrend: historically where the best bases finish forming.";
      } else if (a21 >= 65 && a200 < 45) {
        divergence = " A short-term thrust inside a damaged structure — rallies like this need weeks of repair before they carry; treat strength as tradable, not trustable.";
      } else if (a21 >= 60 && a200 >= 55) {
        divergence = " Short-term and structural participation agree — the healthiest configuration for holding winners longer.";
      } else if (a21 < 40 && a200 < 45) {
        divergence = " Both time frames are weak — the majority of stocks are in downtrends regardless of what the index prints.";
      }
    }
    paras.push({
      title: "Participation",
      text: `${adv} advancers vs ${dec} decliners${advPct != null ? ` (${advPct}% up)` : ""} · ${a21 != null ? `${Math.round(a21)}% of stocks above the 21-EMA` : ""}${a200 != null ? `, ${Math.round(a200)}% above the 200-SMA` : ""} · ${posture.new_52w_highs} new 52-week highs against ${posture.new_52w_lows} new lows.${divergence}`,
    });
  }

  // --- Leadership stress test: are breakouts being PAID? ---
  const working = ev?.breakouts_working ?? [];
  const failed = ev?.breakouts_failed ?? [];
  if (working.length || failed.length) {
    const total = working.length + failed.length;
    const holdRate = total > 0 ? Math.round((working.length / total) * 100) : null;
    const best = [...working].sort((a, b) => b.pct_vs_pivot - a.pct_vs_pivot).slice(0, 3);
    const worst = [...failed].sort((a, b) => a.pct_vs_pivot - b.pct_vs_pivot)[0];
    const bounced = ev?.ema_tests?.bounced ?? [];
    const sliced = ev?.ema_tests?.sliced ?? [];
    const dipRead =
      bounced.length + sliced.length >= 5
        ? bounced.length >= sliced.length * 1.5
          ? ` Dip-buyers are present: ${bounced.length} leaders bounced off the 21-EMA vs ${sliced.length} that sliced through.`
          : sliced.length >= bounced.length * 1.5
            ? ` Dips are NOT being bought: ${sliced.length} leaders sliced through the 21-EMA vs only ${bounced.length} that bounced — distribution behaviour.`
            : ` The 21-EMA test is split (${bounced.length} bounced / ${sliced.length} sliced) — no clear hand in control.`
        : "";
    const verdict =
      holdRate == null
        ? ""
        : holdRate >= 60
          ? "Breakouts are being paid — the single most bullish thing a tape can do."
          : holdRate >= 40
            ? "Breakouts are a coin-flip — buy only the cleanest pivots and take partials fast."
            : "Breakouts are failing — the market is punishing entries; patience beats aggression here.";
    paras.push({
      title: "Leadership stress test",
      text: `${verdict} ${working.length} of ${total} recent base breakouts still hold above their pivots${best.length ? ` — strongest: ${best.map((b) => `${b.symbol} (+${b.pct_vs_pivot.toFixed(1)}% vs pivot)`).join(", ")}` : ""}${worst ? `; worst failure ${worst.symbol} (${worst.pct_vs_pivot.toFixed(1)}%)` : ""}.${dipRead}`,
    });
  }

  // --- Rotation: where is the money going? ---
  const top = env.week_review?.top_sectors ?? [];
  const bottom = env.week_review?.bottom_sectors ?? [];
  if (top.length) {
    paras.push({
      title: "Rotation",
      text: `Money moved into ${top
        .slice(0, 3)
        .map((s) => `${s.sector} (+${s.median_return_5d_pct.toFixed(1)}% median, ${s.stocks} stocks)`)
        .join(", ")}${bottom.length ? ` while ${bottom
        .slice(0, 2)
        .map((s) => `${s.sector} (${s.median_return_5d_pct.toFixed(1)}%)`)
        .join(" and ")} lagged` : ""}. Trade WITH this rotation — your best setups inside the leading sectors carry the group tailwind.`,
    });
  }

  // --- Setup pipeline: next month's breakouts are forming NOW. ---
  if (setupCounts) {
    const pipeline: Array<[string, string]> = [
      ["power-base", "Power Base"],
      ["vcp", "VCP"],
      ["tight-closes", "3 Tight Closes"],
      ["high-tight-flag", "High Tight Flag"],
    ];
    const parts = pipeline
      .filter(([id]) => setupCounts[id] != null)
      .map(([id, label]) => `${setupCounts[id]} ${label}`);
    const totalBases = pipeline.reduce((a, [id]) => a + (setupCounts[id] ?? 0), 0);
    if (parts.length) {
      const read =
        totalBases >= 40
          ? "a RICH pipeline — when breadth confirms, there will be plenty to buy"
          : totalBases >= 15
            ? "a moderate pipeline — selection matters more than aggression"
            : "a thin pipeline — few quality bases means few low-risk entries; forcing trades here is how drawdowns start";
      paras.push({
        title: "Setup pipeline (forward-looking)",
        text: `${totalBases} quality bases are forming right now (${parts.join(", ")}). This is the supply of NEXT month's breakouts — ${read}.`,
      });
    }
  }

  // --- Bottom line: what to actually do. ---
  if (small) {
    const trendBit =
      small.stateScore >= 2
        ? "Smallcap 250 is in a confirmed uptrend"
        : small.stateScore >= 1
          ? "Smallcap 250 is repairing above its 50-DMA"
          : small.stateScore >= -0.5
            ? "Smallcap 250 is pulling back inside a larger uptrend"
            : "Smallcap 250 is in a downtrend";
    const xpBit = xp
      ? xp.xp_score >= 15
        ? "breadth is swing-friendly, so winners can be held for the full move"
        : xp.xp_score >= 12
          ? "breadth is improving but not confirmed — scale in rather than jumping in"
          : "breadth is still choppy, so expect stock-specific moves, quick rotations, and keep position counts low"
      : "";
    const level = small.sma50 !== null ? ` The line in the sand stays the 50-DMA at ${Math.round(small.sma50).toLocaleString("en-IN")}: above it, buy your setups; below it, protect capital first.` : "";
    paras.push({
      title: "Bottom line for the coming weeks",
      text: `${trendBit}${xpBit ? `, and ${xpBit}` : ""}.${level}`,
    });
  }

  return paras.length ? { paras } : null;
}

function Sparkline({ values, tone, height = 44 }: { values: number[]; tone: "pos" | "neu" | "neg"; height?: number }) {
  if (values.length < 2) return null;
  const w = 160;
  const min = Math.min(...values);
  const max = Math.max(...values);
  const span = max - min || 1;
  const pts = values.map((v, i) => `${((i / (values.length - 1)) * w).toFixed(1)},${(height - 4 - ((v - min) / span) * (height - 8)).toFixed(1)}`);
  const color = tone === "pos" ? "var(--positive)" : tone === "neg" ? "var(--negative)" : "var(--amber)";
  return (
    <svg viewBox={`0 0 ${w} ${height}`} className="mko-spark" preserveAspectRatio="none" aria-hidden="true">
      <polyline points={pts.join(" ")} fill="none" stroke={color} strokeWidth="1.8" strokeLinejoin="round" />
      <polygon points={`0,${height} ${pts.join(" ")} ${w},${height}`} fill={color} opacity="0.08" />
    </svg>
  );
}

// Focus-list learning: which sectors the user keeps vs removes, persisted so
// suggestions gradually bias toward the kinds of stocks the user actually wants.
const FOCUS_REMOVED_KEY = "stockScanner.marketsFocusRemoved.v1";
const FOCUS_SECTOR_STATS_KEY = "stockScanner.marketsFocusSectorStats.v1";

function readRemoved(): Set<string> {
  try {
    const raw = JSON.parse(window.localStorage.getItem(FOCUS_REMOVED_KEY) ?? "[]");
    return new Set(Array.isArray(raw) ? raw.map(String) : []);
  } catch {
    return new Set();
  }
}
function readSectorStats(): Record<string, { kept: number; removed: number }> {
  try {
    const raw = JSON.parse(window.localStorage.getItem(FOCUS_SECTOR_STATS_KEY) ?? "{}");
    return raw && typeof raw === "object" ? raw : {};
  } catch {
    return {};
  }
}

type FetchState = "loading" | "ready" | "error";

function num(v: number | null | undefined, digits = 0, suffix = ""): string {
  if (v === null || v === undefined || !Number.isFinite(v)) return "—";
  return v.toFixed(digits) + suffix;
}

function delta(today: number | null | undefined, prev: number | null | undefined): string | null {
  if (today === null || today === undefined || prev === null || prev === undefined) return null;
  const d = today - prev;
  return `${d >= 0 ? "+" : ""}${d.toFixed(1)}`;
}

function verdictClass(verdict: string | undefined): string {
  switch (verdict) {
    case "Press": return "press";
    case "Selective": return "selective";
    case "Protect": return "protect";
    case "Stand Aside": return "aside";
    default: return "";
  }
}

function Spark({ values }: { values: Array<number | null> }) {
  const points = values.filter((v): v is number => v !== null && Number.isFinite(v));
  if (points.length < 3) return null;
  const min = Math.min(...points);
  const max = Math.max(...points);
  const span = max - min || 1;
  const w = 120;
  const h = 30;
  const step = w / (points.length - 1);
  const path = points.map((v, i) => `${i === 0 ? "M" : "L"}${(i * step).toFixed(1)},${(h - ((v - min) / span) * h).toFixed(1)}`).join(" ");
  return (
    <svg className="mk-spark" viewBox={`0 0 ${w} ${h}`} preserveAspectRatio="none" aria-hidden>
      <path d={path} fill="none" stroke="currentColor" strokeWidth="1.6" />
    </svg>
  );
}

// One-line interpretation per metric — words, not numbers.
function ftMeaning(pct: number | null): string {
  if (pct === null) return "Not enough recent breakouts to judge.";
  if (pct >= 65) return "Breakouts are being defended — fresh entries are getting paid.";
  if (pct >= 45) return "Mixed follow-through — be selective, demand the best setups.";
  return "Breakouts are being sold into — fresh breakout buys are fighting the tape.";
}

function qualityMeaning(strong: number | null, faded: number | null): string {
  if (strong === null) return "No breakout attempts today.";
  if ((faded ?? 0) > strong) return "Sellers are using strength — most attempts faded off their highs.";
  if (strong >= 55) return "Strong closes — demand is absorbing supply at the highs.";
  return "Average close quality — no edge either way today.";
}

function emaMeaning(above21: number | null, bouncePct: number | null): string {
  if (above21 === null) return "Leader list too small to judge.";
  const holding = above21 >= 75 ? "Leaders are respecting their EMAs" : above21 >= 55 ? "Leaders are slipping toward their EMAs" : "Leaders are losing their EMAs";
  if (bouncePct !== null) {
    return `${holding}; ${bouncePct >= 60 ? "21 EMA tests are being bought" : "21 EMA tests are failing"}.`;
  }
  return holding + ".";
}

function pressureMeaning(share: number | null): string {
  if (share === null) return "No high-volume moves among leaders today.";
  if (share >= 60) return "Institutions are accumulating the leaders.";
  if (share >= 40) return "Balanced — no clear institutional footprint.";
  return "Distribution — leaders are being sold on volume.";
}

import { SymbolGridModal, type SymbolGridItem } from "./SymbolGridModal";

export function MarketsPanel({
  onOpenSymbolChart,
  onOpenChartWithList,
  xpBreadth = null,
}: {
  onOpenSymbolChart?: (symbol: string) => void;
  onOpenChartWithList?: (symbol: string, symbols: string[]) => void;
  xpBreadth?: XpBreadthScore | null;
}) {
  const [state, setState] = useState<FetchState>("loading");
  const [data, setData] = useState<MarketEnvironmentResponse | null>(null);
  const [indexHealth, setIndexHealth] = useState<Record<string, IndexHealth | null>>({});
  const [indexBars, setIndexBars] = useState<Record<string, ChartBar[]>>({});
  const [setupCounts, setSetupCounts] = useState<Record<string, number> | null>(null);

  useEffect(() => {
    let active = true;
    void Promise.allSettled(
      OUTLOOK_INDICES.map((ix) =>
        getChart(ix.symbol, "1Y", "india").then((resp) => ({
          symbol: ix.symbol,
          bars: resp.bars ?? [],
          health: computeIndexHealth(ix.symbol, ix.label, resp.bars ?? []),
        })),
      ),
    ).then((settled) => {
      if (!active) return;
      const nextHealth: Record<string, IndexHealth | null> = {};
      const nextBars: Record<string, ChartBar[]> = {};
      for (const item of settled) {
        if (item.status === "fulfilled") {
          nextHealth[item.value.symbol] = item.value.health;
          nextBars[item.value.symbol] = item.value.bars;
        }
      }
      setIndexHealth(nextHealth);
      setIndexBars(nextBars);
    });
    // Setup pipeline: how many quality bases are forming right now — the
    // forward supply of next month's breakouts.
    void getScanCounts("india")
      .then((descriptors) => {
        if (!active) return;
        const counts: Record<string, number> = {};
        for (const d of descriptors) counts[d.id] = d.hit_count;
        setSetupCounts(counts);
      })
      .catch(() => {
        if (active) setSetupCounts(null);
      });
    return () => {
      active = false;
    };
  }, []);
  const [removed, setRemoved] = useState<Set<string>>(() => readRemoved());
  const [sectorStats, setSectorStats] = useState<Record<string, { kept: number; removed: number }>>(() => readSectorStats());

  const load = () => {
    setState("loading");
    getMarketEnvironment()
      .then((resp) => {
        setData(resp);
        setState("ready");
      })
      .catch(() => setState("error"));
  };

  useEffect(load, []);

  const bumpSector = (sector: string | undefined, field: "kept" | "removed") => {
    if (!sector) return;
    setSectorStats((prev) => {
      const next = { ...prev, [sector]: { ...(prev[sector] ?? { kept: 0, removed: 0 }) } };
      next[sector][field] += 1;
      try {
        window.localStorage.setItem(FOCUS_SECTOR_STATS_KEY, JSON.stringify(next));
      } catch { /* ignore */ }
      return next;
    });
  };

  const [gridModal, setGridModal] = useState<{ title: string; subtitle?: string; items: SymbolGridItem[] } | null>(null);

  // Open the full app chart, arming ↑/↓ navigation through the given list when
  // the host provides it; falls back to a plain single-symbol open.
  const openChart = (symbol: string, list?: string[]) => {
    if (list && list.length && onOpenChartWithList) onOpenChartWithList(symbol, list);
    else onOpenSymbolChart?.(symbol);
  };

  const removeFocus = (symbol: string, sector?: string) => {
    setRemoved((prev) => {
      const next = new Set(prev);
      next.add(symbol);
      try {
        window.localStorage.setItem(FOCUS_REMOVED_KEY, JSON.stringify([...next]));
      } catch { /* ignore */ }
      return next;
    });
    bumpSector(sector, "removed");
  };

  const restoreFocus = (symbol: string) => {
    setRemoved((prev) => {
      const next = new Set(prev);
      next.delete(symbol);
      try {
        window.localStorage.setItem(FOCUS_REMOVED_KEY, JSON.stringify([...next]));
      } catch { /* ignore */ }
      return next;
    });
  };

  // Learned sector affinity: keep-rate per sector, blended toward neutral until
  // there's enough signal. >0 = user tends to keep this sector, <0 = removes it.
  const sectorAffinity = useMemo(() => {
    const out: Record<string, number> = {};
    for (const [sector, s] of Object.entries(sectorStats)) {
      const total = s.kept + s.removed;
      if (total < 2) continue;
      out[sector] = Math.round(((s.kept - s.removed) / total) * 100);
    }
    return out;
  }, [sectorStats]);

  const today: MarketEnvDay | null = data?.today ?? null;
  const yesterday = data?.yesterday ?? null;

  const compareRows = useMemo(() => {
    if (!today) return [];
    const weekRow = (key: (d: MarketEnvDay) => number | null) => {
      const values = (data?.history ?? [])
        .slice(0, -1)
        .slice(-5)
        .map(() => null); // per-metric weekly averages come from slim history below
      void values;
      return null;
    };
    void weekRow;
    const week = (data?.history ?? []).slice(0, -1).slice(-5);
    const weekAvg = (pick: (r: { structural_held_pct?: number | null; ft3_held_pct: number | null; above_ema21_pct: number | null; score: number | null }) => number | null) => {
      const vals = week.map(pick).filter((v): v is number => v !== null && Number.isFinite(v));
      return vals.length ? vals.reduce((a, b) => a + b, 0) / vals.length : null;
    };
    return [
      {
        label: "Base breakouts still above pivot %",
        today: (today.structural ?? {}).held_pct ?? null,
        yesterday: (yesterday?.structural ?? {}).held_pct ?? null,
        week: weekAvg((r) => r.structural_held_pct ?? null),
      },
      {
        label: "Leaders above 21 EMA %",
        today: today.ema_health?.above_ema21_pct ?? null,
        yesterday: yesterday?.ema_health?.above_ema21_pct ?? null,
        week: weekAvg((r) => r.above_ema21_pct),
      },
      {
        label: "Environment score",
        today: today.score,
        yesterday: yesterday?.score ?? null,
        week: weekAvg((r) => r.score),
      },
    ];
  }, [data, today, yesterday]);

  // Learn once per weekly review cycle: names suggested last week that the
  // user did NOT remove are credited to their sectors as "kept".
  useEffect(() => {
    const reviewed = data?.focus_review?.reviewed_date;
    if (!reviewed) return;
    const marker = `stockScanner.marketsFocusLearned.${reviewed}`;
    try {
      if (window.localStorage.getItem(marker)) return;
    } catch { return; }
    const rows = data?.focus_review?.rows ?? [];
    if (!rows.length) return;
    setSectorStats((prev) => {
      const next = { ...prev };
      for (const r of rows) {
        if (!r.sector || removed.has(r.symbol)) continue;
        next[r.sector] = { ...(next[r.sector] ?? { kept: 0, removed: 0 }) };
        next[r.sector].kept += 1;
      }
      try {
        window.localStorage.setItem(FOCUS_SECTOR_STATS_KEY, JSON.stringify(next));
        window.localStorage.setItem(marker, "1");
      } catch { /* ignore */ }
      return next;
    });
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [data?.focus_review?.reviewed_date]);

  // Display focus: drop removed, re-rank with learned sector affinity, split
  // into kept vs removed so the user can restore.
  const focusRaw = data?.focus ?? [];
  const focusVisible = useMemo(() => {
    return focusRaw
      .filter((f) => !removed.has(f.symbol))
      .map((f) => ({ ...f, adjScore: f.score + (sectorAffinity[f.sector] ?? 0) * 0.15 }))
      .sort((a, b) => b.adjScore - a.adjScore);
  }, [focusRaw, removed, sectorAffinity]);
  const focusRemovedRows = focusRaw.filter((f) => removed.has(f.symbol));

  if (state === "loading" && !data) {
    // Skeleton mirrors the loaded layout: score badge, posture strip, chart.
    return (
      <Panel title="Markets" subtitle="Daily follow-through health of the tape" className="markets-panel">
        <div className="mk-skeleton" aria-label="Loading market environment" role="status">
          <div style={{ display: "flex", gap: 14, alignItems: "center" }}>
            <div className="skeleton" style={{ width: 104, height: 76, borderRadius: 12 }} />
            <div className="skeleton" style={{ width: 220, height: 16 }} />
          </div>
          <div style={{ display: "grid", gridTemplateColumns: "repeat(5, minmax(0, 1fr))", gap: 10, marginTop: 16 }}>
            {Array.from({ length: 5 }, (_, i) => (
              <div key={i} className="skeleton" style={{ height: 56 }} />
            ))}
          </div>
          <div className="skeleton" style={{ height: 220, marginTop: 16 }} />
        </div>
      </Panel>
    );
  }
  if (state === "error" && !data) {
    return (
      <Panel title="Markets" subtitle="Daily follow-through health of the tape" className="markets-panel">
        <div className="mk-loading">
          Could not load market environment. <button type="button" onClick={load}>Retry</button>
        </div>
      </Panel>
    );
  }
  if (!today) return null;

  const structural = (today.structural ?? {}) as Record<string, number | null>;
  const ft3 = (today.followthrough?.d3 ?? {}) as Record<string, number | null>;
  const ft1 = (today.followthrough?.d1 ?? {}) as Record<string, number | null>;
  const ft5 = (today.followthrough?.d5 ?? {}) as Record<string, number | null>;
  const quality = today.close_quality ?? {};
  const ema = today.ema_health ?? {};
  const pressure = today.volume_pressure ?? {};
  const expansion = today.range_expansion ?? {};
  const thrust = today.thrust ?? {};
  const scoreDelta = delta(today.score, yesterday?.score ?? null);
  const ai = data?.ai ?? null;
  const week = data?.week_review;
  const outlook = buildOutlook(xpBreadth, data, indexHealth);
  const underneath = buildUnderneath(data, setupCounts, indexHealth["NIFTYSMLCAP250.NS"] ?? null, xpBreadth);
  const smallcap = indexHealth["NIFTYSMLCAP250.NS"] ?? null;
  const smallcapNote = smallcap
    ? (() => {
        const trendBit =
          smallcap.stateScore >= 2
            ? "is in a confirmed uptrend"
            : smallcap.stateScore >= 1
              ? "is attempting a recovery above its 50-DMA"
              : smallcap.stateScore >= -0.5
                ? "is pulling back inside a larger uptrend"
                : "is in a downtrend";
        const tapeBit = xpBreadth
          ? xpBreadth.xp_score >= 15
            ? "and breadth supports an index-wide advance"
            : xpBreadth.xp_score >= 12
              ? "while breadth is only slowly improving — expect a grind, not a melt-up"
              : "but the broad tape is choppy — expect stock-specific moves rather than an index-wide run"
          : "";
        const levelBit =
          smallcap.sma50 !== null
            ? smallcap.above50
              ? `Holding above the 50-DMA (${Math.round(smallcap.sma50).toLocaleString("en-IN")}) keeps that view intact; losing it turns the coming weeks corrective.`
              : `Reclaiming the 50-DMA (${Math.round(smallcap.sma50).toLocaleString("en-IN")}) is the trigger that would turn the coming weeks constructive.`
            : "";
        return `Smallcap 250 ${trendBit} ${tapeBit}. ${levelBit}`;
      })()
    : null;

  return (
    <Panel
      title="Markets"
      subtitle={`Follow-through health · ${data?.date ?? ""} · ${today.universe} liquid stocks measured`}
      className="markets-panel"
    >
      {/* ===== Market Outlook — the backbone ===== */}
      {outlook ? (
        <section className="mko-hero" aria-label="Market outlook">
          <div className={`mko-verdict mko-${outlook.tone}`}>
            <div className="mko-verdict-kicker"><Compass size={13} strokeWidth={2.2} /> Outlook · next 2–6 weeks</div>
            <div className="mko-verdict-word">{outlook.verdict}</div>
            <div className="mko-verdict-score">
              <div className="mko-meter" role="img" aria-label={`Outlook score ${outlook.score} of 100`}>
                <div className="mko-meter-fill" style={{ width: `${Math.round((outlook.score + 100) / 2)}%` }} />
                <div className="mko-meter-mid" />
              </div>
              <span>{outlook.score > 0 ? `+${outlook.score}` : outlook.score} / ±100</span>
            </div>
            <p className="mko-guidance">{outlook.guidance}</p>
          </div>
          <div className="mko-why">
            <div className="mko-why-title">Why — the three heaviest pieces of evidence</div>
            <ul>
              {outlook.reasons.map((r) => (
                <li key={r}>{r}</li>
              ))}
            </ul>
            <div className="mko-why-title mko-flip-title">What would change this view</div>
            <ul className="mko-flips">
              {outlook.flips.map((f) => (
                <li key={f}>{f}</li>
              ))}
            </ul>
          </div>
        </section>
      ) : null}

      {/* ===== Index charts — Smallcap 250 & Midcap 150, full candles ===== */}
      {OUTLOOK_INDICES.filter((ix) => ix.chart).map((ix) => {
        const h = indexHealth[ix.symbol];
        const bars = indexBars[ix.symbol] ?? [];
        if (!h || bars.length < 2) return null;
        const tone: "pos" | "neu" | "neg" = h.stateScore >= 1 ? "pos" : h.stateScore <= -1 ? "neg" : "neu";
        const StateIcon = h.stateScore >= 1 ? ArrowUpRight : h.stateScore <= -1 ? ArrowDownRight : Minus;
        const featured = ix.symbol === "NIFTYSMLCAP250.NS";
        return (
          <section key={ix.symbol} className="mko-chart-card" aria-label={`${h.label} chart`}>
            <div className="mko-index-head">
              <div>
                <strong>{h.label}</strong>
                <span className="mko-index-price">{h.last.toLocaleString("en-IN", { maximumFractionDigits: 2 })}</span>
              </div>
              <div className="mko-index-stats">
                <span title="Return over the last 20 sessions">
                  20d <strong className={h.ret20Pct !== null && h.ret20Pct >= 0 ? "pos" : "neg"}>{num(h.ret20Pct, 1, "%")}</strong>
                </span>
                <span title="Return over the last 60 sessions">
                  60d <strong className={h.ret60Pct !== null && h.ret60Pct >= 0 ? "pos" : "neg"}>{num(h.ret60Pct, 1, "%")}</strong>
                </span>
                <span title="Distance below the 52-week high">
                  off 52wH <strong>{num(h.distFrom52wHighPct, 1, "%")}</strong>
                </span>
                <span className={`mko-state mko-${tone}`}>
                  <StateIcon size={12} strokeWidth={2.4} /> {h.state}
                </span>
              </div>
            </div>
            <IndexCandleChart bars={bars} height={featured ? 360 : 280} />
            {featured && smallcapNote ? <p className="mko-featured-note">{smallcapNote}</p> : null}
            {featured && smallcap ? (
              <div className="mko-levels">
                <span>Levels that matter:</span>
                {smallcap.sma20 !== null ? <em>20DMA {Math.round(smallcap.sma20).toLocaleString("en-IN")}</em> : null}
                {smallcap.sma50 !== null ? <em>50DMA {Math.round(smallcap.sma50).toLocaleString("en-IN")}</em> : null}
                {smallcap.sma200 !== null ? <em>200DMA {Math.round(smallcap.sma200).toLocaleString("en-IN")}</em> : null}
              </div>
            ) : null}
          </section>
        );
      })}

      {/* ===== Underneath the surface — the full internals read ===== */}
      {underneath ? (
        <section className="mko-under" aria-label="Underneath the surface">
          <div className="mko-signals-head">Underneath the surface — what the leaders and internals are actually doing</div>
          {underneath.paras.map((p) => (
            <div key={p.title} className="mko-under-para">
              <strong>{p.title}</strong>
              <p>{p.text}</p>
            </div>
          ))}
          {(data?.evidence?.breakouts_working?.length || data?.evidence?.breakouts_failed?.length) ? (
            <div className="mko-under-chips">
              {(data?.evidence?.breakouts_working ?? []).slice(0, 8).map((b) => (
                <button
                  key={`w-${b.symbol}`}
                  type="button"
                  className="mko-chip mko-chip-pos"
                  onClick={() => onOpenSymbolChart?.(b.symbol)}
                  title={`Broke out ${b.sessions_ago}d ago from a ${b.base_len_label} base · pivot ${b.pivot} · click to open chart`}
                >
                  {b.symbol} +{b.pct_vs_pivot.toFixed(1)}%
                </button>
              ))}
              {(data?.evidence?.breakouts_failed ?? []).slice(0, 6).map((b) => (
                <button
                  key={`f-${b.symbol}`}
                  type="button"
                  className="mko-chip mko-chip-neg"
                  onClick={() => onOpenSymbolChart?.(b.symbol)}
                  title={`Failed breakout from ${b.sessions_ago}d ago · pivot ${b.pivot} · click to open chart`}
                >
                  {b.symbol} {b.pct_vs_pivot.toFixed(1)}%
                </button>
              ))}
            </div>
          ) : null}
        </section>
      ) : null}

      {/* ===== The evidence table ===== */}
      {outlook ? (
        <section className="mko-signals" aria-label="Outlook evidence">
          <div className="mko-signals-head">The evidence — every signal, its weight, and the logic</div>
          {outlook.signals.map((s) => {
            const tone = s.score > 0.2 ? "pos" : s.score < -0.2 ? "neg" : "neu";
            return (
              <div key={s.label} className="mko-signal-row">
                <span className={`mko-dot mko-${tone}`} aria-label={tone === "pos" ? "bullish" : tone === "neg" ? "bearish" : "neutral"} />
                <div className="mko-signal-main">
                  <div className="mko-signal-top">
                    <strong>{s.label}</strong>
                    <span className="mko-signal-value">{s.valueLabel}</span>
                    <span className="mko-signal-weight" title={`Weight ${s.weight} of the model`}>w{s.weight}</span>
                  </div>
                  <div className="mko-signal-logic">{s.logic}</div>
                </div>
              </div>
            );
          })}
          <div className="mk-footnote mko-footnote">
            Weight-of-evidence model computed live from breadth, trend and follow-through data. Probabilities, not prophecy — when the evidence changes, the outlook changes with it.
          </div>
        </section>
      ) : null}

      <div className="mko-section-hdr">Under the hood — today's counted metrics</div>

      {/* Verdict header */}
      <div className="mk-header">
        <div className={`mk-score ${verdictClass(today.verdict)}`}>
          <strong>{num(today.score, 1)}</strong>
          <span className="mk-verdict">{today.verdict}</span>
        </div>
        <div className="mk-header-context">
          <div>
            {scoreDelta ? (
              <span className={Number(scoreDelta) >= 0 ? "pos" : "neg"}>{scoreDelta} vs yesterday</span>
            ) : (
              <span className="mk-muted">first recorded session</span>
            )}
            {data?.week_avg_score !== null && data?.week_avg_score !== undefined ? (
              <span className="mk-muted"> · last-week avg {num(data.week_avg_score, 1)}</span>
            ) : null}
          </div>
          <Spark values={(data?.history ?? []).map((h) => h.score)} />
        </div>
        {ai?.one_rule_today ? <div className="mk-rule">Rule today: {ai.one_rule_today}</div> : null}
      </div>

      {/* Market posture strip */}
      {data?.posture ? (
        <div className="mk-posture">
          <div className="mk-posture-item">
            <span>Adv / Dec</span>
            <strong><em className="pos">{data.posture.advances}</em> / <em className="neg">{data.posture.declines}</em></strong>
          </div>
          <div className="mk-posture-item">
            <span>52w High / Low today</span>
            <strong><em className="pos">{data.posture.new_52w_highs}</em> / <em className="neg">{data.posture.new_52w_lows}</em></strong>
          </div>
          <div className="mk-posture-item">
            <span>&gt; 21 EMA</span>
            <strong>{num(data.posture.above_ema21_pct, 0, "%")}</strong>
          </div>
          <div className="mk-posture-item">
            <span>&gt; 50 SMA</span>
            <strong>{num(data.posture.above_sma50_pct, 0, "%")}</strong>
          </div>
          <div className="mk-posture-item">
            <span>&gt; 200 SMA</span>
            <strong>{num(data.posture.above_sma200_pct, 0, "%")}</strong>
          </div>
          <div className="mk-posture-note">
            52w/MA stats measured on {data.posture.leveled_universe ?? data.posture.universe} stocks with
            verified-fresh levels; stale-history names are excluded, not guessed.
          </div>
        </div>
      ) : null}

      {/* Breadth trend — is participation improving? */}
      {(() => {
        const series = (data?.history ?? [])
          .filter((h) => h.date)
          .map((h) => ({
            date: (h.date ?? "").slice(5),
            "> 21 EMA": h.above_ema21_pct ?? null,
            "> 50 SMA": h.above_sma50_pct ?? null,
            "> 200 SMA": h.above_sma200_pct ?? null,
          }));
        if (series.length < 2) {
          return (
            <div className="mk-breadth">
              <div className="mk-week-hdr">Breadth Trend</div>
              <div className="mk-muted">Building — the multi-day breadth chart needs a few sessions of history. Today's snapshot is in the posture strip above.</div>
            </div>
          );
        }
        return (
          <div className="mk-breadth">
            <div className="mk-week-hdr">Breadth Trend — % of stocks above key moving averages</div>
            <div className="mk-breadth-chart">
              <ResponsiveContainer width="100%" height={220}>
                <LineChart data={series} margin={{ top: 8, right: 12, bottom: 4, left: -18 }}>
                  <CartesianGrid stroke="var(--line)" strokeDasharray="3 3" vertical={false} />
                  <XAxis dataKey="date" tick={{ fontSize: 11, fill: "var(--text-muted)" }} />
                  <YAxis domain={[0, 100]} tick={{ fontSize: 11, fill: "var(--text-muted)" }} />
                  <Tooltip contentStyle={{ fontSize: 12 }} />
                  <Legend wrapperStyle={{ fontSize: 12 }} />
                  {/* isAnimationActive=false: the draw-in animation freezes mid-way
                      when rAF is throttled (background tab, battery saver), leaving
                      a stuck stroke-dasharray and an apparently empty chart. */}
                  <Line type="monotone" dataKey="> 21 EMA" stroke="#00d2ff" strokeWidth={2} dot={false} connectNulls isAnimationActive={false} />
                  <Line type="monotone" dataKey="> 50 SMA" stroke="#f7b955" strokeWidth={2} dot={false} connectNulls isAnimationActive={false} />
                  <Line type="monotone" dataKey="> 200 SMA" stroke="#089981" strokeWidth={2} dot={false} connectNulls isAnimationActive={false} />
                </LineChart>
              </ResponsiveContainer>
            </div>
            <div className="mk-footnote">
              Rising lines = broadening participation (healthy); falling while the index holds = a narrowing,
              distribution-prone tape. The 200 SMA line is the slow, structural one; the 21 EMA line is the fast swing gauge.
            </div>
          </div>
        );
      })()}

      {/* AI daily read */}
      {ai ? (
        <div className="mk-ai">
          {ai.headline ? <div className="mk-ai-headline">{ai.headline}</div> : null}
          {(ai.narrative ?? []).map((p, i) => (
            <p key={i}>{p}</p>
          ))}
        </div>
      ) : (
        // Quiet footnote, not a boxed error: the absence of the AI read is
        // routine (rate limits, cold start) and shouldn't look like a fault.
        <div className="mk-footnote" style={{ marginBottom: 14 }}>
          AI read unavailable right now — the counted metrics below stand on their own.
        </div>
      )}

      {/* Today vs yesterday vs week */}
      <div className="mk-compare">
        <div className="mk-compare-head">
          <span>Metric</span><span>Today</span><span>Yesterday</span><span>Week avg</span>
        </div>
        {compareRows.map((row) => (
          <div key={row.label} className="mk-compare-row">
            <span>{row.label}</span>
            <strong>{num(row.today, 1)}</strong>
            <span>{num(row.yesterday, 1)}</span>
            <span>{row.week !== null ? num(row.week, 1) : "—"}</span>
          </div>
        ))}
      </div>

      {/* Component cards */}
      <div className="mk-grid">
        <div className="mk-card">
          <div className="mk-card-hdr">Base Breakout Follow-Through</div>
          <div className="mk-big">{num(structural.held_pct ?? null, 0, "%")}<small> of {structural.events ?? 0} base breakouts (last ~12 sessions) still above pivot</small></div>
          <div className="mk-sub">
            back inside base: {num(structural.back_in_base_pct ?? null, 0, "%")} · short-term clears held (1d/3d/5d): {num(ft1.held_pct, 0, "%")} / {num(ft3.held_pct, 0, "%")} / {num(ft5.held_pct, 0, "%")}
          </div>
          <div className="mk-meaning">{ftMeaning(structural.held_pct ?? null)}</div>
          <Spark values={(data?.history ?? []).map((h) => h.structural_held_pct ?? null)} />
        </div>
        <div className="mk-card">
          <div className="mk-card-hdr">Today's Breakout Quality</div>
          <div className="mk-big">{num(quality.strong_pct, 0, "%")}<small> strong closes of {quality.count ?? 0} attempts</small></div>
          <div className="mk-sub">faded below midpoint: {num(quality.faded_pct, 0, "%")}</div>
          <div className="mk-meaning">{qualityMeaning(quality.strong_pct ?? null, quality.faded_pct ?? null)}</div>
        </div>
        <div className="mk-card">
          <div className="mk-card-hdr">Leader EMA Health</div>
          <div className="mk-big">{num(ema.above_ema21_pct, 0, "%")}<small> of {ema.leaders ?? 0} leaders above 21 EMA</small></div>
          <div className="mk-sub">
            above 10 EMA: {num(ema.above_ema10_pct, 0, "%")} · 21 EMA tests bought: {num(ema.ema21_bounce_pct, 0, "%")} of {ema.ema21_touches ?? 0}
          </div>
          <div className="mk-meaning">{emaMeaning(ema.above_ema21_pct ?? null, ema.ema21_bounce_pct ?? null)}</div>
          <Spark values={(data?.history ?? []).map((h) => h.above_ema21_pct)} />
        </div>
        <div className="mk-card">
          <div className="mk-card-hdr">Leader Volume Pressure</div>
          <div className="mk-big">
            {num(pressure.accumulation_share_pct, 0, "%")}
            <small> accumulation share ({pressure.accumulation ?? 0} up / {pressure.distribution ?? 0} down on volume)</small>
          </div>
          <div className="mk-meaning">{pressureMeaning(pressure.accumulation_share_pct ?? null)}</div>
        </div>
        <div className="mk-card">
          <div className="mk-card-hdr">Range Expansion Direction</div>
          <div className="mk-big">{num(expansion.up_share_pct, 0, "%")}<small> of wide-range days closed up ({expansion.up ?? 0} vs {expansion.down ?? 0})</small></div>
          <div className="mk-meaning">
            {expansion.up_share_pct === null ? "No unusually wide days today." : (expansion.up_share_pct ?? 0) >= 60 ? "The big candles belong to buyers." : (expansion.up_share_pct ?? 0) <= 40 ? "The big candles belong to sellers." : "Big-range days are split — no side in control."}
          </div>
        </div>
        <div className="mk-card">
          <div className="mk-card-hdr">Thrust &amp; Tape</div>
          <div className="mk-big">
            {thrust.up_4pct ?? 0} <small>up 4%+</small> / {thrust.down_4pct ?? 0} <small>down 4%+</small>
          </div>
          <div className="mk-sub">
            up/down volume {num(thrust.updown_volume_ratio, 2, "x")} · fresh 20d highs {thrust.fresh_20d_highs ?? 0} vs lows {thrust.fresh_20d_lows ?? 0}
          </div>
          <div className="mk-meaning">
            {(thrust.up_4pct ?? 0) >= (thrust.down_4pct ?? 0) * 2 ? "Momentum aggression is one-sided to the upside." : (thrust.down_4pct ?? 0) >= (thrust.up_4pct ?? 0) * 2 ? "Downside aggression dominates — momentum longs are swimming upstream." : "Two-way tape — aggression is balanced."}
          </div>
        </div>
      </div>

      {/* Open positions health */}
      {(data?.positions ?? []).length ? (
        <div className="mk-week">
          <div className="mk-week-hdr">Your Open Positions — health check</div>
          <div className="mk-pos-list">
            {(data?.positions ?? []).map((p) => (
              <div key={p.symbol + String(p.avg_px)} className={`mk-pos-row cat-${p.category.toLowerCase().replace(/[^a-z]+/g, "-")}`}>
                <button type="button" className="mk-symbol" onClick={() => p.mapped && onOpenSymbolChart?.(p.symbol)}>
                  {p.symbol}
                </button>
                <span className="mk-pos-cat">{p.category}</span>
                {p.pnl_pct !== null && p.pnl_pct !== undefined ? (
                  <strong className={p.pnl_pct >= 0 ? "pos" : "neg"}>
                    {p.pnl_pct >= 0 ? "+" : ""}{p.pnl_pct.toFixed(1)}%
                  </strong>
                ) : <strong>—</strong>}
                <small>
                  {p.mapped ? `avg ${p.avg_px} → ${p.last_price}` : `avg ${p.avg_px}`}
                  {p.rs_rating ? ` · RS ${p.rs_rating}` : ""}
                </small>
                <div className="mk-pos-advice">{p.advice}</div>
              </div>
            ))}
          </div>
          <div className="mk-footnote">
            Positions are netted from your journal's buy/sell entries and re-classified daily against the same
            rules as the market metrics. Worst conditions listed first.
          </div>
        </div>
      ) : (
        <div className="mk-week">
          <div className="mk-week-hdr">Your Open Positions</div>
          <div className="mk-muted">
            No open positions synced yet — open the Journal page once (it syncs your positions to the backend), then revisit.
          </div>
        </div>
      )}

      {/* Leaders + sector-breakout cards */}
      <div className="mk-cardrow">
        {(data?.leaders ?? []).length ? (
          <div className="mk-bigcard">
            <div className="mk-bigcard-num">{data?.leaders?.length ?? 0}</div>
            <div className="mk-bigcard-label">Market Leaders</div>
            <div className="mk-bigcard-sub">
              {(data?.leaders ?? []).filter((l) => l.above_ema21).length} above their 21 EMA · 2%/5% circuit-band names excluded
            </div>
            <div className="mk-bigcard-actions">
              <button
                type="button"
                onClick={() => {
                  const syms = (data?.leaders ?? []).map((l) => l.symbol);
                  openChart(syms[0], syms);
                }}
              >
                Full chart (↑/↓ steps all)
              </button>
              <button
                type="button"
                onClick={() =>
                  setGridModal({
                    title: "Market Leaders",
                    subtitle: `${data?.leaders?.length ?? 0} Stage-2 leaders · click any chart to open it full`,
                    items: (data?.leaders ?? []).map((l) => ({
                      symbol: l.symbol,
                      name: l.name,
                      badge: l.rs_rating ? `RS ${l.rs_rating}` : undefined,
                      badgeTone: "pos",
                      note: `${l.above_ema21 ? "above" : "below"} 21 EMA · ${l.pct_from_52w_high.toFixed(1)}% off high`,
                    })),
                  })
                }
              >
                ⊞ Grid view
              </button>
            </div>
          </div>
        ) : null}
        {(data?.sector_breakouts ?? []).length ? (
          <div className="mk-bigcard">
            <div className="mk-bigcard-num">{data?.sector_breakouts?.length ?? 0}</div>
            <div className="mk-bigcard-label">Sector Breakouts Setting Up</div>
            <div className="mk-bigcard-sub">Leading-sector names 0–5% under a pivot — the next to fire</div>
            <div className="mk-bigcard-actions">
              <button
                type="button"
                onClick={() => {
                  const syms = (data?.sector_breakouts ?? []).map((b) => b.symbol);
                  openChart(syms[0], syms);
                }}
              >
                Full chart (↑/↓ steps all)
              </button>
              <button
                type="button"
                onClick={() =>
                  setGridModal({
                    title: "Leading-sector breakouts, about to fire",
                    subtitle: "Names in the strongest sectors coiled 0–5% under a base pivot",
                    items: (data?.sector_breakouts ?? []).map((b) => ({
                      symbol: b.symbol,
                      name: b.name,
                      badge: `${b.pct_below_pivot.toFixed(1)}% to pivot`,
                      badgeTone: "muted",
                      note: `${b.sector} · pivot ${b.pivot}`,
                    })),
                  })
                }
              >
                ⊞ Grid view
              </button>
            </div>
          </div>
        ) : null}
      </div>

      {/* Focus list — 40+ names with a buy plan, removable, learns your taste */}
      {focusRaw.length ? (
        <div className="mk-week">
          <div className="mk-week-hdr">Focus for the coming week — {focusVisible.length} names, each with a plan</div>
          {Object.keys(sectorAffinity).length ? (
            <div className="mk-affinity">
              Learned from your edits:
              {Object.entries(sectorAffinity)
                .sort((a, b) => b[1] - a[1])
                .slice(0, 6)
                .map(([sector, v]) => (
                  <span key={sector} className={`mk-tag ${v >= 0 ? "pos-tag" : "neg-tag"}`}>
                    {sector} {v >= 0 ? "↑" : "↓"}
                  </span>
                ))}
            </div>
          ) : null}
          <div className="mk-focus-grid">
            {focusVisible.map((f) => (
              <div key={f.symbol} className="mk-focus-card">
                <div className="mk-focus-top">
                  <button
                    type="button"
                    className="mk-symbol"
                    onClick={() => openChart(f.symbol, focusVisible.map((x) => x.symbol))}
                  >
                    {f.symbol}
                  </button>
                  <span className={f.change_pct >= 0 ? "pos" : "neg"}>
                    {f.change_pct >= 0 ? "+" : ""}{f.change_pct.toFixed(1)}%
                  </span>
                  <button
                    type="button"
                    className="mk-focus-remove"
                    aria-label={`Remove ${f.symbol}`}
                    title="Remove — the page learns your preference"
                    onClick={() => removeFocus(f.symbol, f.sector)}
                  >
                    ×
                  </button>
                </div>
                <div className="mk-focus-setup">
                  {f.setup ?? "Setup"} · <span className="mk-muted">{f.sector}</span>
                </div>
                {f.entry ? (
                  <div className="mk-focus-plan">
                    <div><em>Buy:</em> {f.entry}</div>
                    <div><em>Stop:</em> {f.stop}</div>
                    {f.buy_note ? <div className="mk-muted">{f.buy_note}</div> : null}
                  </div>
                ) : null}
                <div className="mk-focus-tags">
                  {f.reasons.map((r) => <span key={r} className="mk-tag">{r}</span>)}
                </div>
              </div>
            ))}
          </div>
          {focusRemovedRows.length ? (
            <div className="mk-removed">
              Removed ({focusRemovedRows.length}):
              {focusRemovedRows.map((f) => (
                <button key={f.symbol} type="button" className="mk-tag mk-restore" onClick={() => restoreFocus(f.symbol)}>
                  {f.symbol} ↺
                </button>
              ))}
            </div>
          ) : null}
          <div className="mk-footnote">
            Selection: RS ≥ 72–80, above a stacked 50/200 SMA, within 18% of the 52-week high, liquid. Each card shows
            the setup and a concrete plan — a watch list, not a buy list. Remove any you don't want; the page learns
            which sectors you keep and re-ranks future lists toward them.
          </div>
        </div>
      ) : null}

      {/* Weekly focus review — did last week's picks do what we thought? */}
      {data?.focus_review?.summary ? (
        <div className="mk-week">
          <div className="mk-week-hdr">
            Focus scorecard — the list from {data.focus_review.reviewed_date}, graded
          </div>
          <div className="mk-review-summary">
            <strong className={data.focus_review.summary.avg_return_pct >= 0 ? "pos" : "neg"}>
              {data.focus_review.summary.avg_return_pct >= 0 ? "+" : ""}
              {data.focus_review.summary.avg_return_pct.toFixed(1)}% avg
            </strong>
            <span>{data.focus_review.summary.worked}/{data.focus_review.summary.count} behaved as expected (≥3%) · {data.focus_review.summary.hit_rate_pct.toFixed(0)}% hit rate</span>
          </div>
          <div className="mk-review-grid">
            {data.focus_review.rows.slice(0, 20).map((r) => (
              <div key={r.symbol} className={`mk-review-row detailed ${r.worked ? "won" : "lost"}`}>
                <div className="mk-review-head">
                  <button type="button" className="mk-symbol" onClick={() => onOpenSymbolChart?.(r.symbol)}>{r.symbol}</button>
                  <strong className={r.return_pct >= 0 ? "pos" : "neg"}>{r.return_pct >= 0 ? "+" : ""}{r.return_pct.toFixed(1)}%</strong>
                  <small>{r.setup}</small>
                </div>
                {r.why ? <div className="mk-review-why">{r.why}</div> : null}
                {r.strategy ? <div className="mk-review-strategy">{r.strategy}</div> : null}
              </div>
            ))}
          </div>
          <div className="mk-footnote">
            Return since the suggestion day, unmanaged. "Behaved as expected" = a tradable follow-through of +3% or
            more — the goal is that the setups fire, not that every one is green.
          </div>
        </div>
      ) : null}

      {/* Named evidence */}
      <div className="mk-week">
        <div className="mk-week-hdr">The Evidence — names, not claims</div>
        <div className="mk-week-grid">
          <div>
            <div className="mk-week-sub pos-hdr">Breakouts working ({(data?.evidence?.breakouts_working ?? []).length})</div>
            {(data?.evidence?.breakouts_working ?? []).map((e) => (
              <div key={e.symbol} className="mk-week-row">
                <button type="button" className="mk-symbol" onClick={() => onOpenSymbolChart?.(e.symbol)}>{e.symbol}</button>
                <strong className="pos">+{e.pct_vs_pivot.toFixed(1)}%</strong>
                <small>vs pivot {e.pivot} · broke {e.sessions_ago}s ago · base {e.base_len_label}</small>
              </div>
            ))}
            {(data?.evidence?.breakouts_working ?? []).length === 0 ? <div className="mk-muted">None in the last ~12 sessions.</div> : null}
          </div>
          <div>
            <div className="mk-week-sub neg-hdr">Back inside the base ({(data?.evidence?.breakouts_failed ?? []).length})</div>
            {(data?.evidence?.breakouts_failed ?? []).map((e) => (
              <div key={e.symbol} className="mk-week-row">
                <button type="button" className="mk-symbol" onClick={() => onOpenSymbolChart?.(e.symbol)}>{e.symbol}</button>
                <strong className="neg">{e.pct_vs_pivot.toFixed(1)}%</strong>
                <small>vs pivot {e.pivot} · broke {e.sessions_ago}s ago · base {e.base_len_label}</small>
              </div>
            ))}
            {(data?.evidence?.breakouts_failed ?? []).length === 0 ? <div className="mk-muted">None — breakouts are holding.</div> : null}
          </div>
        </div>
        <div className="mk-week-grid mk-ema-tests">
          <div>
            <div className="mk-week-sub pos-hdr">21 EMA tests bought</div>
            <div className="mk-chip-row">
              {(data?.evidence?.ema_tests?.bounced ?? []).map((e) => (
                <button key={e.symbol} type="button" className="mk-chip pos-chip" onClick={() => onOpenSymbolChart?.(e.symbol)}>
                  {e.symbol} <small>+{e.pct_vs_ema21.toFixed(1)}%</small>
                </button>
              ))}
            </div>
          </div>
          <div>
            <div className="mk-week-sub neg-hdr">21 EMA tests failed</div>
            <div className="mk-chip-row">
              {(data?.evidence?.ema_tests?.sliced ?? []).map((e) => (
                <button key={e.symbol} type="button" className="mk-chip neg-chip" onClick={() => onOpenSymbolChart?.(e.symbol)}>
                  {e.symbol} <small>{e.pct_vs_ema21.toFixed(1)}%</small>
                </button>
              ))}
            </div>
          </div>
        </div>
      </div>

      {/* Last week review */}
      <div className="mk-week">
        <div className="mk-week-hdr">Last Week — what worked, what didn't</div>
        <div className="mk-week-grid">
          <div>
            <div className="mk-week-sub pos-hdr">Worked</div>
            {ai?.what_worked?.length ? (
              <ul>{ai.what_worked.map((w, i) => <li key={i}>{w}</li>)}</ul>
            ) : null}
            {(week?.top_sectors ?? []).map((s) => (
              <div key={s.sector} className="mk-week-row">
                <span>{s.sector}</span>
                <strong className={s.median_return_5d_pct >= 0 ? "pos" : "neg"}>
                  {s.median_return_5d_pct >= 0 ? "+" : ""}{s.median_return_5d_pct.toFixed(1)}%
                </strong>
                <small>sector median 5d</small>
              </div>
            ))}
          </div>
          <div>
            <div className="mk-week-sub neg-hdr">Didn't</div>
            {ai?.what_didnt?.length ? (
              <ul>{ai.what_didnt.map((w, i) => <li key={i}>{w}</li>)}</ul>
            ) : null}
            {(week?.bottom_sectors ?? []).map((s) => (
              <div key={s.sector} className="mk-week-row">
                <span>{s.sector}</span>
                <strong className={s.median_return_5d_pct >= 0 ? "pos" : "neg"}>
                  {s.median_return_5d_pct >= 0 ? "+" : ""}{s.median_return_5d_pct.toFixed(1)}%
                </strong>
                <small>sector median 5d</small>
              </div>
            ))}
          </div>
        </div>
        <div className="mk-footnote">
          Sector rows are the median 5-day return across each sector's liquid stocks. Day-vs-day and week
          comparisons deepen automatically as daily history accumulates.
        </div>
      </div>

      {gridModal ? (
        <SymbolGridModal
          title={gridModal.title}
          subtitle={gridModal.subtitle}
          items={gridModal.items}
          market="india"
          onOpenSymbolChart={(sym) => {
            setGridModal(null);
            onOpenSymbolChart?.(sym);
          }}
          onClose={() => setGridModal(null)}
        />
      ) : null}
    </Panel>
  );
}
