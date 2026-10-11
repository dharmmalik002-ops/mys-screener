import { describe, expect, it } from "vitest";

import type { QuarterlyResultEntry, ResultsBasis } from "./api";
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
  showBasis,
  showUpcomingBasis,
  type ShownResult,
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
    standalone: {
      metrics: { sales: metric(salesYoy), operating_profit: metric(null), net_profit: metric(null), eps: metric(null) },
      operating_margin_pct: null,
      pe: null,
      peg: null,
      result_url: null,
    },
    consolidated: null,
    ...extra,
  };
}

function shown(item: QuarterlyResultEntry, basis: ResultsBasis = "consolidated"): ShownResult {
  const out = showBasis(item, basis);
  if (!out) throw new Error("no basis");
  return out;
}

describe("results page rules", () => {
  const items = [
    entry("AAA", "2026-10-08T15:00", 500, 5),
    entry("BBB", "2026-10-10T17:42", 100, null),
    entry("CCC", "2026-10-10T17:42", 900, 30, { cap_bucket: "mid", sector: "Banks" }),
    entry("DDD", "2026-10-09T10:00", 50, -12),
  ].map((item) => shown(item));

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

  it("shows the chosen basis, and the other one only when a company does not file it", () => {
    const base = entry("TCS", "2026-10-09T15:40", 800000, 11.2);
    const metric = (value: number, yoy: number) => ({ value, prev: null, year_ago: null, yoy_pct: yoy, qoq_pct: null });
    const consolidated = {
      metrics: { sales: metric(73188, 11.23), operating_profit: metric(1, 1), net_profit: metric(1, 1), eps: metric(1, 1) },
      operating_margin_pct: 25,
      pe: 15.16,
      peg: 2.05,
      result_url: null,
    };
    const both = { ...base, consolidated };
    expect(shown(both, "consolidated").shown_basis).toBe("consolidated");
    expect(shown(both, "consolidated").metrics.sales.value).toBe(73188);
    expect(shown(both, "standalone").metrics.sales.yoy_pct).toBe(11.2);
    // No subsidiaries: standalone, marked as such.
    expect(shown(base, "consolidated").shown_basis).toBe("standalone");
    // Filed only on NSE: consolidated even when standalone was asked for.
    expect(shown({ ...both, standalone: null }, "standalone").shown_basis).toBe("consolidated");
    expect(showBasis({ ...base, standalone: null }, "consolidated")).toBeNull();

    const upcoming = { symbol: "X", name: "X", sector: null, sub_sector: null, market_cap_crore: 1, cap_bucket: null, date: "2026-10-20",
      standalone: { last_period: "Jun 2026", last_sales_yoy_pct: 5, last_profit_yoy_pct: 6, pe: 10, peg: null }, consolidated: null };
    expect(showUpcomingBasis(upcoming, "consolidated")).toMatchObject({ shown_basis: "standalone", last_sales_yoy_pct: 5 });
    expect(showUpcomingBasis({ ...upcoming, standalone: null }, "consolidated")).toMatchObject({ shown_basis: null, last_period: null });
  });
});
