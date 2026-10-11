import { describe, expect, it } from "vitest";

import type { QuarterlyResultEntry } from "./api";
import {
  DEFAULT_FILTERS,
  filterResults,
  formatCrore,
  formatDeclared,
  formatMarketCap,
  formatPct,
  groupByDay,
  rangePresets,
  seasonStart,
  sortResults,
  todayIst,
} from "./results";

function entry(symbol: string, declared: string, cap: number, salesYoy: number | null, extra: Partial<QuarterlyResultEntry> = {}): QuarterlyResultEntry {
  const metric = (yoy: number | null) => ({ value: 1, prev: 1, year_ago: 1, yoy_pct: yoy, qoq_pct: null });
  return {
    symbol,
    name: `${symbol} Ltd`,
    sector: "Tech",
    sub_sector: null,
    market_cap_crore: cap,
    cap_bucket: "large",
    declared_at: declared,
    declared_date: declared.slice(0, 10),
    period: "Sep 2026",
    prev_period: "Jun 2026",
    year_ago_period: "Sep 2025",
    metrics: { sales: metric(salesYoy), operating_profit: metric(null), net_profit: metric(null), eps: metric(null) },
    operating_margin_pct: null,
    pe: null,
    peg: null,
    result_url: null,
    ...extra,
  };
}

describe("results page rules", () => {
  const items = [
    entry("AAA", "2026-10-08T15:00", 500, 5),
    entry("BBB", "2026-10-10T17:42", 100, null),
    entry("CCC", "2026-10-10T17:42", 900, 30, { cap_bucket: "mid", sector: "Banks" }),
    entry("DDD", "2026-10-09T10:00", 50, -12),
  ];

  it("lists the newest declaration first, larger companies first in a tie", () => {
    expect(sortResults(items, "latest").map((i) => i.symbol)).toEqual(["CCC", "BBB", "DDD", "AAA"]);
  });

  it("sorts growth highest first with unmeasurable growth last", () => {
    expect(sortResults(items, "sales").map((i) => i.symbol)).toEqual(["CCC", "AAA", "DDD", "BBB"]);
  });

  it("filters by size, sector, watchlist and search together", () => {
    const watch = new Set(["AAA", "CCC"]);
    expect(filterResults(items, { ...DEFAULT_FILTERS, caps: ["mid"] }, watch).map((i) => i.symbol)).toEqual(["CCC"]);
    expect(filterResults(items, { ...DEFAULT_FILTERS, sectors: ["Tech"], scope: "watchlist" }, watch).map((i) => i.symbol)).toEqual(["AAA"]);
    expect(filterResults(items, { ...DEFAULT_FILTERS, search: "ddd l" }, watch).map((i) => i.symbol)).toEqual(["DDD"]);
  });

  it("groups by declared day in list order", () => {
    const groups = groupByDay(sortResults(items, "latest"));
    expect(groups.map((g) => [g.date, g.items.length])).toEqual([["2026-10-10", 2], ["2026-10-09", 1], ["2026-10-08", 1]]);
  });

  it("knows the season and the presets", () => {
    expect(seasonStart("2026-11-14")).toBe("2026-10-01");
    expect(seasonStart("2026-01-03")).toBe("2026-01-01");
    const presets = Object.fromEntries(rangePresets("2026-10-11").map((p) => [p.key, [p.from, p.to]]));
    expect(presets.week).toEqual(["2026-10-05", "2026-10-11"]);
    expect(presets.season).toEqual(["2026-10-01", "2026-10-11"]);
    expect(presets["last-season"]).toEqual(["2026-07-01", "2026-09-30"]);
  });

  it("reads today in IST, not UTC", () => {
    expect(todayIst(new Date("2026-10-10T19:00:00Z"))).toBe("2026-10-11"); // 00:30 IST
    expect(todayIst(new Date("2026-10-10T18:00:00Z"))).toBe("2026-10-10"); // 23:30 IST
  });

  it("formats like the exchange: crore, lakh crore, IST filing time", () => {
    expect(formatCrore(19644)).toBe("19,644");
    expect(formatCrore(11.4)).toBe("11.40");
    expect(formatCrore(null)).toBe("—");
    expect(formatMarketCap(230000)).toBe("₹2.30 L Cr.");
    expect(formatMarketCap(45210.4)).toBe("₹45,210 Cr.");
    expect(formatPct(-13.6)).toBe("-13.60%");
    expect(formatDeclared("2026-10-09T17:42")).toBe("9 Oct 2026 · 5:42 PM");
    expect(formatDeclared("2026-10-09T00:05")).toBe("9 Oct 2026 · 12:05 AM");
  });
});
