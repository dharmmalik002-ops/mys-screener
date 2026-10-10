import { describe, expect, it } from "vitest";
import type { MarketEnvironmentResponse, MarketsExposure, XpBreadthScore } from "./api";
import {
  breadthAnswer,
  breakoutAnswer,
  conditionFromExposure,
  conditionFromXp,
  disagreement,
  freshness,
  leadershipAnswer,
  planAnswer,
  readCondition,
  readablePct,
  siteCondition,
  trendAnswer,
  type IndexRead,
} from "./marketsBrief";

function exposure(pct: number | null, direction: "improving" | "deteriorating" | "stable" | "unknown" = "unknown", extra: Partial<MarketsExposure> = {}): MarketsExposure {
  return {
    available: pct !== null,
    reason: null,
    as_of_session: "2026-10-09",
    stats_as_of_session: "2026-10-08",
    stats_lag_days: 1,
    verdict: pct === null ? null : ({ available: true, exposure_pct: pct, direction } as MarketsExposure["verdict"]),
    edge_trend: [],
    context: { participation: null, xp_regime: null, distribution_days: null },
    sources: {},
    methodology: "",
    ...extra,
  };
}

function xp(score: number, regime = "Avoid Longs"): XpBreadthScore {
  return { date: "2026-10-09", xp_score: score, regime, regime_color: "#e03131", universe: null, history: [], bands: [] };
}

function index(label: string, above50: boolean, above200: boolean, state = "Downtrend", asOf = "2026-10-09"): IndexRead {
  return { label, last: 100, dayPct: 0, above50, above200, state, asOf };
}

describe("R1 — one condition word", () => {
  it("maps the exposure ladder to four words", () => {
    expect(conditionFromExposure(25).word).toBe("Defensive");
    expect(conditionFromExposure(50).word).toBe("Cautious");
    expect(conditionFromExposure(75).word).toBe("Constructive");
    expect(conditionFromExposure(100).word).toBe("Offensive");
  });

  it("uses XP only as a stand-in when the exposure rule is unavailable", () => {
    expect(readCondition(exposure(50), xp(30)).word).toBe("Cautious");
    expect(readCondition(exposure(50), xp(30)).basis).toBe("exposure");
    expect(readCondition(exposure(null), xp(30)).basis).toBe("breadth");
    expect(readCondition(null, null).word).toBe("Unclear");
  });

  it("XP bands match the gauge", () => {
    expect(conditionFromXp(5).word).toBe("Defensive");
    expect(conditionFromXp(9.5).word).toBe("Cautious");
    expect(conditionFromXp(15).word).toBe("Constructive");
    expect(conditionFromXp(25.1).word).toBe("Offensive");
  });
});

describe("R2 — disagreement never changes the word", () => {
  it("notes a cautious word against rising indices", () => {
    const condition = conditionFromExposure(25);
    const note = disagreement(condition, [index("A", true, true), index("B", true, true), index("C", false, false)]);
    expect(note).toMatch(/^Mixed:/);
    expect(condition.word).toBe("Defensive");
  });

  it("is silent when the evidence agrees or there is too little of it", () => {
    expect(disagreement(conditionFromExposure(25), [index("A", false, false), index("B", false, false)])).toBeNull();
    expect(disagreement(conditionFromExposure(25), [index("A", true, true)])).toBeNull();
  });
});

describe("R4 — a small sample is not a reading", () => {
  it("refuses a percentage on fewer than eight events", () => {
    expect(readablePct(60, 7)).toBe("too few to read");
    expect(readablePct(60, 8)).toBe("60%");
    expect(readablePct(null, 100)).toBe("—");
  });
});

describe("R5 — say how old the numbers are", () => {
  it("names an index that is a session behind", () => {
    const f = freshness([index("Nifty 50", true, true, "x", "2026-10-09"), index("Smallcap 250", true, true, "x", "2026-10-08")], exposure(50));
    expect(f.asOf).toBe("2026-10-09");
    expect(f.notes.join(" ")).toContain("Smallcap 250 is a session behind");
  });

  it("flags a breakout replay more than four days old", () => {
    const f = freshness([], exposure(50, "unknown", { stats_lag_days: 6, stats_as_of_session: "2026-10-01" }));
    expect(f.notes.join(" ")).toContain("6 days old");
  });
});

describe("section answers", () => {
  it("trend lists every index and tones by how many are rising", () => {
    expect(trendAnswer([])).toBeNull();
    const all = trendAnswer([{ label: "A", state: "Confirmed Uptrend" }, { label: "B", state: "Attempting Recovery" }]);
    expect(all?.tone).toBe("pos");
    expect(all?.text).toBe("A: Confirmed Uptrend · B: Attempting Recovery");
    expect(trendAnswer([{ label: "A", state: "Downtrend" }])?.tone).toBe("neg");
  });

  it("breadth falls back to the history figure and omits what it lacks", () => {
    expect(breadthAnswer(null)).toBeNull();
    expect(breadthAnswer(null, 61.4)?.text).toBe("61% of stocks above their 50-day");
    expect(breadthAnswer(null, 61.4)?.tone).toBe("pos");
  });

  it("breakouts respect the sample rule", () => {
    const env = (held: number, events: number) =>
      ({ today: { structural: { held_pct: held, events } } }) as unknown as MarketEnvironmentResponse;
    expect(breakoutAnswer(env(70, 3))?.text).toMatch(/Too few/);
    expect(breakoutAnswer(env(70, 40))?.tone).toBe("pos");
    expect(breakoutAnswer(env(50, 40))?.text).toMatch(/mixed$/);
    expect(breakoutAnswer(env(30, 40))?.tone).toBe("neg");
  });

  it("leadership names the top two each way", () => {
    const env = {
      week_review: {
        top_sectors: [{ sector: "A" }, { sector: "B" }, { sector: "C" }],
        bottom_sectors: [{ sector: "Z" }],
      },
    } as unknown as MarketEnvironmentResponse;
    expect(leadershipAnswer(env)?.text).toBe("Leading: A, B · Lagging: Z (last 5 sessions)");
  });

  it("plan pluralises", () => {
    expect(planAnswer(0, 1).text).toBe("No open positions synced · 1 name on the focus list");
    expect(planAnswer(2, 40).text).toBe("2 open positions · 40 names on the focus list");
  });
});

describe("one vocabulary across Home and Markets", () => {
  it("leads with the exposure word and keeps XP as a supporting reading", () => {
    const site = siteCondition(exposure(50, "improving"), xp(6.03));
    expect(site.condition.word).toBe("Cautious");
    expect(site.size).toBe("50% of full size");
    expect(site.direction).toBe("improving");
    expect(site.supporting).toBe("XP breadth 6.0 · Avoid Longs");
  });

  it("does not repeat XP as 'supporting' when XP is the stand-in", () => {
    const site = siteCondition(exposure(null), xp(20, "Swing Friendly"));
    expect(site.condition.basis).toBe("breadth");
    expect(site.condition.word).toBe("Constructive");
    expect(site.size).toBeNull();
    expect(site.supporting).toBeNull();
  });

  it("reads 'stable' as 'steady' and drops an unknown direction", () => {
    expect(siteCondition(exposure(75, "stable"), null).direction).toBe("steady");
    expect(siteCondition(exposure(75, "unknown"), null).direction).toBeNull();
  });
});
