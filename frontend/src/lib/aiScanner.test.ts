import { describe, expect, it } from "vitest";
import type { AiScanRequest, AiScanRow } from "./api";
import { clearCriterion, editLabel, formatMetric, listColumn, sortRows, withCriterionValue, withPattern, withoutPattern } from "./aiScanner";

const base: AiScanRequest = {
  patterns: ["vcp"],
  pattern_match: "any",
  max_pct_from_52w_high: 20,
  above_ema50: true,
  growth_quarters: 3,
  price_vs_ma_mode: "below",
};

describe("AI scanner criteria", () => {
  it("removing a chip resets the filter to its off value", () => {
    expect(clearCriterion(base, { key: "max_pct_from_52w_high", kind: "max" }).max_pct_from_52w_high).toBeNull();
    expect(clearCriterion(base, { key: "above_ema50", kind: "bool" }).above_ema50).toBe(false);
    expect(clearCriterion(base, { key: "growth_quarters", kind: "int" }).growth_quarters).toBe(1);
    expect(clearCriterion(base, { key: "price_vs_ma_mode", kind: "enum" }).price_vs_ma_mode).toBe("any");
  });

  it("an edited number is read leniently and rejected when it is not a number", () => {
    expect(withCriterionValue(base, { key: "max_pct_from_52w_high", kind: "max" }, "15%")?.max_pct_from_52w_high).toBe(15);
    expect(withCriterionValue(base, { key: "growth_quarters", kind: "int" }, "2.6")?.growth_quarters).toBe(3);
    expect(withCriterionValue(base, { key: "max_pct_from_52w_high", kind: "max" }, "abc")).toBeNull();
    expect(withCriterionValue(base, { key: "max_pct_from_52w_high", kind: "max" }, "  ")).toBeNull();
  });

  it("the edit label keeps the comparison", () => {
    expect(editLabel({ text: "Sales growth YoY ≥ 20%", kind: "min" })).toBe("Sales growth YoY ≥");
    expect(editLabel({ text: "Growth held 3 quarters in a row", kind: "int" })).toBe("Quarters in a row");
  });

  it("patterns are added once and removed by id", () => {
    expect(withPattern(base, "vcp").patterns).toEqual(["vcp"]);
    expect(withPattern(base, "cup-handle").patterns).toEqual(["vcp", "cup-handle"]);
    expect(withoutPattern(base, "vcp").patterns).toEqual([]);
  });
});

describe("AI scanner table", () => {
  it("formats growth with a sign and distances without one", () => {
    expect(formatMetric(23.456, { id: "sales_yoy", format: "pct" })).toBe("+23.5%");
    expect(formatMetric(4, { id: "from_52w_high", format: "pct" })).toBe("4.0%");
    expect(formatMetric(null, { id: "sales_yoy", format: "pct" })).toBe("—");
  });

  it("missing values sink whichever way the column is sorted", () => {
    const row = (symbol: string, value: number | null) =>
      ({ symbol, last_price: 1, change_pct: 0, score: 0, metrics: { sales_yoy: value } }) as unknown as AiScanRow;
    const rows = [row("A", 10), row("B", null), row("C", 30)];
    expect(sortRows(rows, { id: "sales_yoy", dir: "desc" }).map((r) => r.symbol)).toEqual(["C", "A", "B"]);
    expect(sortRows(rows, { id: "sales_yoy", dir: "asc" }).map((r) => r.symbol)).toEqual(["A", "C", "B"]);
  });
});

describe("AI scanner research list", () => {
  it("shows the first thing asked about that the list does not already carry", () => {
    const columns = [
      { id: "price", label: "Price", format: "price" as const },
      { id: "market_cap", label: "M cap", format: "crore" as const },
      { id: "sales_yoy", label: "Sales YoY", format: "pct" as const },
    ];
    expect(listColumn(columns)).toMatchObject({ id: "sales_yoy", short: "Sales" });
    expect(listColumn(columns.slice(0, 2))).toBeNull();
  });
});
