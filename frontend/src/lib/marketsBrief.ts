/**
 * What the Markets page says first, and the rules for saying it.
 *
 * The page used to open with ~20 stacked sections (exposure arithmetic, four
 * context tiles, a macro essay, three charts, twelve weighted signals, six
 * metric cards, two focus lists...). Each was defensible; together they buried
 * the three things a trader opens this page for: what is the market doing, how
 * much risk should I carry, and what would change my mind.
 *
 * Everything here is a declared rule, not a judgement call made at render time:
 *
 *  R1  ONE condition word, from the page's single verdict (the exposure rule).
 *      The XP breadth score only stands in when the exposure rule cannot be
 *      computed. The two are never averaged and the word is never AI-written —
 *      a model's sentence must not be able to contradict the number above it.
 *  R2  Say so when the evidence disagrees. If trend and the exposure rule point
 *      opposite ways, the word does not change; a one-line "mixed" note is added.
 *  R3  Omit, do not dash. A figure that could not be computed is left out, so
 *      the page never shows a row of "—".
 *  R4  A small sample is not a reading. A percentage built on fewer than
 *      MIN_SAMPLE events is reported as "too few to read".
 *  R5  Say how old the numbers are. The as-of session is always shown, and a
 *      source that is behind the others is named.
 */
import type { MarketEnvironmentResponse, MarketsExposure, XpBreadthScore } from "./api";

export type Tone = "pos" | "neu" | "neg";
export type ConditionWord = "Defensive" | "Cautious" | "Constructive" | "Offensive" | "Unclear";

export type Condition = {
  word: ConditionWord;
  tone: Tone;
  /** One sentence, fixed per word. Never generated, so it can never disagree with the word. */
  guidance: string;
  /** Which input produced the word. */
  basis: "exposure" | "breadth" | "none";
};

export type IndexRead = {
  label: string;
  last: number;
  /** Today's move, from the last two closes. Null when only one close is known. */
  dayPct: number | null;
  above50: boolean;
  above200: boolean;
  /** "Confirmed Uptrend" | "Attempting Recovery" | "Pullback / Basing" | "Downtrend" */
  state: string;
  /** ISO date (YYYY-MM-DD) of the last close the figures come from. */
  asOf: string | null;
};

export type BriefFact = { label: string; value: string; tone?: Tone };

/** Below this many events a percentage is noise (see R4). */
export const MIN_SAMPLE = 8;

const GUIDANCE: Record<ConditionWord, string> = {
  Defensive: "Breakouts are not paying right now. Keep new longs few and small, and let cash do the work.",
  Cautious: "Selective only. Take the cleanest setups at reduced size and give up on the marginal ones.",
  Constructive: "Conditions support normal-size entries on good setups. Keep stops disciplined.",
  Offensive: "Conditions are strong. Full-size entries on A setups are supported.",
  Unclear: "There is not enough data to read the market yet.",
};

const TONE: Record<ConditionWord, Tone> = {
  Defensive: "neg",
  Cautious: "neu",
  Constructive: "pos",
  Offensive: "pos",
  Unclear: "neu",
};

function build(word: ConditionWord, basis: Condition["basis"]): Condition {
  return { word, tone: TONE[word], guidance: GUIDANCE[word], basis };
}

/** R1 — exposure ladder is 25 / 50 / 75 / 100. */
export function conditionFromExposure(exposurePct: number): Condition {
  const word: ConditionWord =
    exposurePct <= 25 ? "Defensive" : exposurePct <= 50 ? "Cautious" : exposurePct <= 75 ? "Constructive" : "Offensive";
  return build(word, "exposure");
}

/** Same bands the XP breadth gauge uses (9.5 / 15 / 25). */
export function conditionFromXp(xpScore: number): Condition {
  const word: ConditionWord =
    xpScore > 25 ? "Offensive" : xpScore >= 15 ? "Constructive" : xpScore >= 9.5 ? "Cautious" : "Defensive";
  return build(word, "breadth");
}

export function readCondition(exposure: MarketsExposure | null, xp: XpBreadthScore | null): Condition {
  const pct = exposure?.available && exposure.verdict?.available ? exposure.verdict.exposure_pct : null;
  if (pct !== null && pct !== undefined) return conditionFromExposure(pct);
  if (xp && Number.isFinite(xp.xp_score)) return conditionFromXp(xp.xp_score);
  return build("Unclear", "none");
}

/**
 * R2 — null when the evidence agrees (or there are too few indices to say).
 * The condition word itself is never changed by this.
 */
export function disagreement(condition: Condition, indices: IndexRead[]): string | null {
  if (indices.length < 2) return null;
  const trendingUp = indices.filter((i) => i.above50 && i.above200).length;
  const belowFifty = indices.filter((i) => !i.above50).length;
  const total = indices.length;
  const cautious = condition.word === "Defensive" || condition.word === "Cautious";
  const positive = condition.word === "Constructive" || condition.word === "Offensive";
  if (cautious && trendingUp >= 2) {
    return `Mixed: ${trendingUp} of ${total} indices are still above their 50 and 200-day averages, but breakouts are not paying. Size by the exposure number; the trend says the dip may be shallow.`;
  }
  if (positive && belowFifty >= 2) {
    return `Mixed: breakouts are paying, but ${belowFifty} of ${total} indices are below their 50-day average. Keep size down until the indices recover it.`;
  }
  return null;
}

function pctText(value: number | null | undefined, digits = 0): string | null {
  return value === null || value === undefined || !Number.isFinite(value) ? null : `${value.toFixed(digits)}%`;
}

/** R3 — only facts that exist. Order is the order a trader reads them. */
export function breadthFacts(env: MarketEnvironmentResponse | null, xp: XpBreadthScore | null): BriefFact[] {
  const facts: BriefFact[] = [];
  const posture = env?.posture;
  if (posture && posture.advances + posture.declines > 0) {
    facts.push({
      label: "Advancing / declining",
      value: `${posture.advances.toLocaleString("en-IN")} / ${posture.declines.toLocaleString("en-IN")}`,
      tone: posture.advances >= posture.declines ? "pos" : "neg",
    });
  }
  const above50 = pctText(posture?.above_sma50_pct);
  if (above50) facts.push({ label: "Above 50-day", value: above50, tone: (posture?.above_sma50_pct ?? 0) >= 50 ? "pos" : "neg" });
  const above200 = pctText(posture?.above_sma200_pct);
  if (above200) facts.push({ label: "Above 200-day", value: above200, tone: (posture?.above_sma200_pct ?? 0) >= 50 ? "pos" : "neg" });
  if (posture && posture.new_52w_highs + posture.new_52w_lows > 0) {
    facts.push({
      label: "52-week highs / lows",
      value: `${posture.new_52w_highs} / ${posture.new_52w_lows}`,
      tone: posture.new_52w_highs >= posture.new_52w_lows ? "pos" : "neg",
    });
  }
  if (xp && Number.isFinite(xp.xp_score)) {
    facts.push({ label: "XP breadth score", value: `${xp.xp_score.toFixed(1)} · ${xp.regime}` });
  }
  return facts;
}

/** R4 */
export function readablePct(value: number | null | undefined, sample: number | null | undefined, digits = 0): string {
  if (value === null || value === undefined || !Number.isFinite(value)) return "—";
  if ((sample ?? 0) < MIN_SAMPLE) return "too few to read";
  return `${value.toFixed(digits)}%`;
}

/** R5 — the session the page is describing, and anything lagging behind it. */
export function freshness(
  indices: IndexRead[],
  exposure: MarketsExposure | null,
  staleAfterDays = 4,
): { asOf: string | null; notes: string[] } {
  const dates = indices.map((i) => i.asOf).filter((d): d is string => Boolean(d)).sort();
  const asOf = dates.length ? dates[dates.length - 1] : exposure?.as_of_session ?? null;
  const notes: string[] = [];
  for (const index of indices) {
    if (index.asOf && asOf && index.asOf < asOf) notes.push(`${index.label} is a session behind (${index.asOf}).`);
  }
  const lag = exposure?.stats_lag_days ?? 0;
  if (lag > staleAfterDays) {
    notes.push(`The breakout replay behind the exposure number is ${lag} days old (last session ${exposure?.stats_as_of_session}).`);
  }
  return { asOf, notes };
}

/** "2026-10-08" -> "8 Oct 2026" */
export function longDate(iso: string | null | undefined): string | null {
  if (!iso) return null;
  const parsed = new Date(`${iso}T00:00:00`);
  return Number.isNaN(parsed.getTime())
    ? null
    : parsed.toLocaleDateString("en-IN", { day: "numeric", month: "short", year: "numeric" });
}

/** The IST calendar date of a chart bar (bars are stamped at 00:00 UTC of the session date). */
export function barDate(time: number): string {
  return new Date(time * 1000).toISOString().slice(0, 10);
}
