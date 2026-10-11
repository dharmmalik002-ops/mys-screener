import { describe, expect, it } from "vitest";
import {
  DEFAULT_INDEX_OVERLAY,
  capBand,
  indexCandidates,
  isOverlayIndexSymbol,
  normalizeIndexOverlaySettings,
  placeIndexAboveBars,
  resolveIndexChoice,
} from "./indexOverlay";

describe("capBand", () => {
  it("splits at the SEBI rank-100 and rank-250 market caps", () => {
    expect(capBand(1_796_647)).toBe("large");
    expect(capBand(100_000)).toBe("large");
    expect(capBand(99_999)).toBe("mid");
    expect(capBand(33_000)).toBe("mid");
    expect(capBand(32_999)).toBe("small");
    expect(capBand(900)).toBe("small");
  });

  it("is unknown without a usable market cap", () => {
    expect(capBand(null)).toBeNull();
    expect(capBand(undefined)).toBeNull();
    expect(capBand(0)).toBeNull();
    expect(capBand(Number.NaN)).toBeNull();
  });
});

describe("resolveIndexChoice", () => {
  it("auto picks the index of the stock's own size band", () => {
    expect(resolveIndexChoice("auto", 500_000)).toBe("nifty50");
    expect(resolveIndexChoice("auto", 50_000)).toBe("midcap100");
    expect(resolveIndexChoice("auto", 5_000)).toBe("smallcap100");
  });

  it("auto falls back to the Nifty 50 when the size is unknown", () => {
    expect(resolveIndexChoice("auto", null)).toBe("nifty50");
  });

  it("an explicit choice wins over the size", () => {
    expect(resolveIndexChoice("nifty50", 5_000)).toBe("nifty50");
    expect(resolveIndexChoice("smallcap100", 500_000)).toBe("smallcap100");
  });
});

describe("index symbols", () => {
  it("asks for the 100-stock index first and labels a stand-in as itself", () => {
    expect(indexCandidates("smallcap100")[0]).toEqual({ symbol: "^CNXSC", label: "Nifty Smallcap 100" });
    expect(indexCandidates("midcap100")[0].label).toBe("Nifty Midcap 100");
    expect(indexCandidates("midcap100")[1].label).toBe("Nifty Midcap 150");
  });

  it("knows an index chart so it draws no line on top of itself", () => {
    expect(isOverlayIndexSymbol("^nsei")).toBe(true);
    expect(isOverlayIndexSymbol("RELIANCE")).toBe(false);
    expect(isOverlayIndexSymbol(null)).toBe(false);
  });
});

describe("normalizeIndexOverlaySettings", () => {
  it("defaults to on, auto, near-black", () => {
    expect(normalizeIndexOverlaySettings(null)).toEqual(DEFAULT_INDEX_OVERLAY);
    expect(DEFAULT_INDEX_OVERLAY.enabled).toBe(true);
  });

  it("keeps valid values and repairs bad ones", () => {
    expect(normalizeIndexOverlaySettings({ enabled: false, choice: "midcap100", color: "#FF0000", opacity: 0.4 })).toEqual({
      enabled: false,
      choice: "midcap100",
      color: "#FF0000",
      opacity: 0.4,
    });
    const repaired = normalizeIndexOverlaySettings({ enabled: "yes", choice: "nasdaq", color: "red", opacity: 7 });
    expect(repaired).toEqual({ enabled: true, choice: "auto", color: DEFAULT_INDEX_OVERLAY.color, opacity: 1 });
    expect(normalizeIndexOverlaySettings({ opacity: 0 }).opacity).toBe(0.1);
  });
});

describe("placeIndexAboveBars", () => {
  // A stock that ranges 100 -> 200 (a 2x year) against an index that moves 10%.
  const stock = [
    { time: 1, high: 105, low: 100 },
    { time: 2, high: 150, low: 140 },
    { time: 3, high: 200, low: 190 },
  ];
  const index = [
    { time: 1, value: 20_000 },
    { time: 2, value: 22_000 },
    { time: 3, value: 21_000 },
  ];

  it("sits above every recent candle, its low point just above the highest one", () => {
    const { points } = placeIndexAboveBars(stock, index, { gap: 0.03 });
    points.forEach((point) => expect(point.value).toBeGreaterThanOrEqual(200 * 1.03 - 1e-9));
    expect(Math.min(...points.map((p) => p.value))).toBeCloseTo(200 * 1.03, 6);
  });

  it("stretches a small index move to a visible share of the stock's range", () => {
    const { points } = placeIndexAboveBars(stock, index, { band: 0.35 });
    const drawnRange = Math.log(Math.max(...points.map((p) => p.value)) / Math.min(...points.map((p) => p.value)));
    // Drawn as is, a 10% index swing would be log(1.1) = 0.095; the stock's range is log(2) = 0.69.
    expect(drawnRange).toBeCloseTo(0.35 * Math.log(200 / 100), 6);
    expect(drawnRange).toBeGreaterThan(Math.log(1.1) * 2);
  });

  it("keeps the order of the index's moves and reads back the real value", () => {
    const placed = placeIndexAboveBars(stock, index);
    expect(placed.points[1].value).toBeGreaterThan(placed.points[2].value);
    expect(placed.points[2].value).toBeGreaterThan(placed.points[0].value);
    placed.points.forEach((point, i) => expect(placed.toIndex(point.value)).toBeCloseTo(index[i].value, 6));
  });

  it("draws nothing without data", () => {
    expect(placeIndexAboveBars([], index).points).toEqual([]);
    expect(placeIndexAboveBars(stock, []).points).toEqual([]);
  });
});
