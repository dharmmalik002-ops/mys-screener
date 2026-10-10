// The index line drawn along the top of a stock chart (MarketSmith's black
// line): Nifty 50 behind a large cap, Midcap 100 behind a mid cap, Smallcap 100
// behind a small cap, so the stock is always read against its own size band.
import { getChart, type ChartBar, type MarketKey } from "./api";

export type IndexOverlayChoice = "auto" | "nifty50" | "midcap100" | "smallcap100";
export type CapBand = "large" | "mid" | "small";

export type IndexOverlaySettings = {
  enabled: boolean;
  choice: IndexOverlayChoice;
  /** #rrggbb */
  color: string;
  /** 0.1 .. 1 */
  opacity: number;
};

export const DEFAULT_INDEX_OVERLAY: IndexOverlaySettings = {
  enabled: true,
  choice: "auto",
  // Near-black, as MarketSmith draws it; every chart palette here is light.
  color: "#111827",
  opacity: 0.75,
};

export const INDEX_OVERLAY_STORAGE_KEY = "stockScanner.indexOverlay.v1";

// SEBI's bands are by rank (top 100 large, next 150 mid, rest small). These are
// the market caps at ranks 100 and 250 of free_universe.json (Rs 1,08,185 cr and
// Rs 32,837 cr in Apr 2026), rounded, so a chart can classify from its own
// summary without loading the universe.
export const LARGE_CAP_MIN_CRORE = 100_000;
export const MID_CAP_MIN_CRORE = 33_000;

export function capBand(marketCapCrore: number | null | undefined): CapBand | null {
  if (typeof marketCapCrore !== "number" || !Number.isFinite(marketCapCrore) || marketCapCrore <= 0) return null;
  if (marketCapCrore >= LARGE_CAP_MIN_CRORE) return "large";
  if (marketCapCrore >= MID_CAP_MIN_CRORE) return "mid";
  return "small";
}

type IndexCandidate = { symbol: string; label: string };

// First symbol is the index asked for; the rest are stand-ins used only when it
// cannot be fetched, and their label says so.
const INDEX_CANDIDATES: Record<Exclude<IndexOverlayChoice, "auto">, IndexCandidate[]> = {
  nifty50: [{ symbol: "^NSEI", label: "Nifty 50" }],
  midcap100: [
    { symbol: "NIFTY_MIDCAP_100.NS", label: "Nifty Midcap 100" },
    { symbol: "NIFTYMIDCAP150.NS", label: "Nifty Midcap 150" },
  ],
  // Yahoo's ^CNXSC is the Smallcap 100 (CLAUDE.md gotcha 151).
  smallcap100: [
    { symbol: "^CNXSC", label: "Nifty Smallcap 100" },
    { symbol: "NIFTYSMLCAP250.NS", label: "Nifty Smallcap 250" },
  ],
};

export const INDEX_OVERLAY_CHOICES: Array<{ key: IndexOverlayChoice; label: string }> = [
  { key: "auto", label: "Auto (by size)" },
  { key: "nifty50", label: "Nifty 50" },
  { key: "midcap100", label: "Midcap 100" },
  { key: "smallcap100", label: "Smallcap 100" },
];

/** Which index to draw. Auto falls back to the Nifty 50 when the size is unknown. */
export function resolveIndexChoice(
  choice: IndexOverlayChoice,
  marketCapCrore: number | null | undefined,
): Exclude<IndexOverlayChoice, "auto"> {
  if (choice !== "auto") return choice;
  const band = capBand(marketCapCrore);
  return band === "small" ? "smallcap100" : band === "mid" ? "midcap100" : "nifty50";
}

export function indexCandidates(choice: Exclude<IndexOverlayChoice, "auto">): IndexCandidate[] {
  return INDEX_CANDIDATES[choice];
}

const INDEX_SYMBOLS = new Set(Object.values(INDEX_CANDIDATES).flat().map((c) => c.symbol));

/** True for a chart that is itself one of the overlay indices — no line on top of itself. */
export function isOverlayIndexSymbol(symbol: string | null | undefined): boolean {
  return Boolean(symbol && INDEX_SYMBOLS.has(symbol.toUpperCase()));
}

export function normalizeIndexOverlaySettings(raw: unknown): IndexOverlaySettings {
  const value = raw && typeof raw === "object" ? (raw as Record<string, unknown>) : {};
  const choice = INDEX_OVERLAY_CHOICES.some((c) => c.key === value.choice)
    ? (value.choice as IndexOverlayChoice)
    : DEFAULT_INDEX_OVERLAY.choice;
  const color = typeof value.color === "string" && /^#[0-9a-f]{6}$/i.test(value.color) ? value.color : DEFAULT_INDEX_OVERLAY.color;
  const opacityRaw = Number(value.opacity);
  const opacity = Number.isFinite(opacityRaw) ? Math.min(1, Math.max(0.1, opacityRaw)) : DEFAULT_INDEX_OVERLAY.opacity;
  return {
    enabled: typeof value.enabled === "boolean" ? value.enabled : DEFAULT_INDEX_OVERLAY.enabled,
    choice,
    color,
    opacity,
  };
}

export function loadIndexOverlaySettings(): IndexOverlaySettings {
  try {
    const saved = window.localStorage.getItem(INDEX_OVERLAY_STORAGE_KEY);
    return saved ? normalizeIndexOverlaySettings(JSON.parse(saved)) : DEFAULT_INDEX_OVERLAY;
  } catch {
    return DEFAULT_INDEX_OVERLAY;
  }
}

export function saveIndexOverlaySettings(settings: IndexOverlaySettings) {
  try {
    window.localStorage.setItem(INDEX_OVERLAY_STORAGE_KEY, JSON.stringify(settings));
  } catch {
    // storage blocked: the setting lasts for this page only
  }
}

export type IndexOverlaySeries = { symbol: string; label: string; bars: ChartBar[] };

// One fetch per index and timeframe for every chart on the page; stepping
// through a list of stocks must not re-download the Nifty each time.
const CACHE_TTL_MS = 10 * 60 * 1000;
const cache = new Map<string, { at: number; promise: Promise<IndexOverlaySeries | null> }>();

export function fetchIndexOverlay(
  choice: Exclude<IndexOverlayChoice, "auto">,
  timeframe: string,
  market: MarketKey,
): Promise<IndexOverlaySeries | null> {
  const key = `${market}:${choice}:${timeframe}`;
  const hit = cache.get(key);
  if (hit && Date.now() - hit.at < CACHE_TTL_MS) return hit.promise;
  const promise = (async () => {
    for (const candidate of indexCandidates(choice)) {
      try {
        const payload = await getChart(candidate.symbol, timeframe, market);
        const bars = (payload?.bars ?? []).filter((bar) => Number.isFinite(bar.close) && bar.close > 0);
        if (bars.length >= 2) return { symbol: candidate.symbol, label: candidate.label, bars };
      } catch {
        // try the stand-in
      }
    }
    return null;
  })();
  cache.set(key, { at: Date.now(), promise });
  // A failure is not cached for the full TTL: retry on the next chart after a minute.
  void promise.then((result) => {
    if (!result) cache.set(key, { at: Date.now() - CACHE_TTL_MS + 60_000, promise });
  });
  return promise;
}
