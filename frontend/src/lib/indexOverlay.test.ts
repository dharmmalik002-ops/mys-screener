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
  const stock = [
    { time: 1, high: 100 },
    { time: 2, high: 110 },
    { time: 3, high: 105 },
  ];
  const index = [
    { time: 1, value: 20_000 },
    { time: 2, value: 20_000 },
    { time: 3, value: 21_000 },
  ];

  it("runs just above every recent candle and touches the gap at the closest one", () => {
    const { points, scale } = placeIndexAboveBars(stock, index, { gap: 0.04 });
    points.forEach((point, i) => expect(point.value).toBeGreaterThanOrEqual(stock[i].high * 1.04 - 1e-9));
    expect(points[1].value).toBeCloseTo(110 * 1.04, 6);
    expect(points[2].value / scale).toBeCloseTo(21_000, 6);
  });

  it("keeps the index's own shape", () => {
    const { points } = placeIndexAboveBars(stock, index);
    expect(points[2].value / points[0].value).toBeCloseTo(21_000 / 20_000, 9);
  });

  it("draws nothing without data", () => {
    expect(placeIndexAboveBars([], index).points).toEqual([]);
    expect(placeIndexAboveBars(stock, []).points).toEqual([]);
  });
});
