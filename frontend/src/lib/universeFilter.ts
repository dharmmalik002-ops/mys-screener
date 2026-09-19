// The universe gate: liquidity, volatility and price floors the user sets ONCE
// and every scanner then obeys.
//
// Each scanner already carries its own thresholds, and several grew their own
// "min liquidity" control over time (gap-up, both Minervini modes, IPO). Those
// stay — they are part of each setup's definition. This is the layer above them:
// the stocks a given trader is willing to touch at all, regardless of which
// pattern found them. Without it the same three judgements get re-made on every
// one of ~25 scanner tabs, differently each time.
//
// It is applied client-side, at the single point where scan results reach the
// screener (`visibleScanItems` in App.tsx), so the count in the metrics strip,
// the table, the distribution charts, the chart grid and the CSV export all
// agree about what the universe is. Nothing here touches the request, so
// changing a floor re-filters instantly without re-running a scan.

import type { ScanMatch } from "./api";

export type UniverseFilter = {
  /** Minimum 20-session average daily range, in percent. */
  minAdrPct: number | null;
  /** Minimum 30-session average traded value, in crore. */
  minTurnoverCrore: number | null;
  /** Minimum last price, in rupees. */
  minPrice: number | null;
};

export const EMPTY_UNIVERSE_FILTER: UniverseFilter = {
  minAdrPct: null,
  minTurnoverCrore: null,
  minPrice: null,
};

export type UniverseFilterOutcome = {
  items: ScanMatch[];
  /** How many rows the gate removed. 0 when no floor is set. */
  removed: number;
  /** Of those, how many were dropped for having no value on record. */
  removedForMissingData: number;
};

export function isUniverseFilterActive(filter: UniverseFilter): boolean {
  return filter.minAdrPct !== null || filter.minTurnoverCrore !== null || filter.minPrice !== null;
}

/** A positive, finite number, or null. Used for both stored values and input. */
function readFloor(value: unknown): number | null {
  if (typeof value !== "number" || !Number.isFinite(value) || value <= 0) {
    return null;
  }
  return value;
}

export function normalizeUniverseFilter(value: unknown): UniverseFilter {
  if (typeof value !== "object" || value === null) {
    return EMPTY_UNIVERSE_FILTER;
  }
  const raw = value as Partial<UniverseFilter>;
  return {
    minAdrPct: readFloor(raw.minAdrPct),
    minTurnoverCrore: readFloor(raw.minTurnoverCrore),
    minPrice: readFloor(raw.minPrice),
  };
}

/** Parse one input box. Empty (or nonsense) means "no floor", not zero. */
export function parseFloorInput(text: string): number | null {
  const trimmed = text.trim();
  if (!trimmed) return null;
  const parsed = Number(trimmed);
  return readFloor(parsed);
}

/** ADR for a row, or null when there is none on record.
 *
 *  The backend defaults `adr_pct_20` to 0.0 rather than leaving it unset, and a
 *  stock whose average daily range is genuinely zero does not exist — so zero
 *  here always means absence. This mirrors `adrOf` in ScanTable; both have to
 *  read a zero the same way or the table will show "—" for a row the gate just
 *  certified as passing an ADR floor.
 */
function adrOf(item: ScanMatch): number | null {
  const adr = item.adr_pct_20;
  return adr == null || adr <= 0 ? null : adr;
}

function turnoverOf(item: ScanMatch): number | null {
  const turnover = item.avg_rupee_volume_30d_crore;
  return turnover == null || turnover <= 0 ? null : turnover;
}

/**
 * Apply the gate.
 *
 * A row whose value is MISSING fails a floor rather than passing it: a floor is
 * a statement about what the user will trade, and an unknown number cannot be
 * shown to clear it. Those rows are counted separately so the UI can say so out
 * loud instead of quietly shrinking the list — a scanner that returned 40 names
 * and displays 12 has to explain the other 28.
 */
export function applyUniverseFilter(items: ScanMatch[], filter: UniverseFilter): UniverseFilterOutcome {
  if (!isUniverseFilterActive(filter)) {
    return { items, removed: 0, removedForMissingData: 0 };
  }

  const kept: ScanMatch[] = [];
  let removedForMissingData = 0;

  for (const item of items) {
    let missing = false;
    let fails = false;

    if (filter.minAdrPct !== null) {
      const adr = adrOf(item);
      if (adr === null) {
        missing = true;
        fails = true;
      } else if (adr < filter.minAdrPct) {
        fails = true;
      }
    }

    if (!fails && filter.minTurnoverCrore !== null) {
      const turnover = turnoverOf(item);
      if (turnover === null) {
        missing = true;
        fails = true;
      } else if (turnover < filter.minTurnoverCrore) {
        fails = true;
      }
    }

    if (!fails && filter.minPrice !== null && item.last_price < filter.minPrice) {
      fails = true;
    }

    if (fails) {
      if (missing) removedForMissingData += 1;
    } else {
      kept.push(item);
    }
  }

  return { items: kept, removed: items.length - kept.length, removedForMissingData };
}

/** One-line summary for the collapsed control, e.g. "ADR 4%+ · 10 Cr+". */
export function describeUniverseFilter(filter: UniverseFilter): string {
  const parts: string[] = [];
  if (filter.minAdrPct !== null) parts.push(`ADR ${trimNumber(filter.minAdrPct)}%+`);
  if (filter.minTurnoverCrore !== null) parts.push(`${trimNumber(filter.minTurnoverCrore)} Cr+`);
  if (filter.minPrice !== null) parts.push(`₹${trimNumber(filter.minPrice)}+`);
  return parts.length ? parts.join(" · ") : "Off";
}

function trimNumber(value: number): string {
  return Number.isInteger(value) ? String(value) : String(Number(value.toFixed(2)));
}
