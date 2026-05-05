import { useEffect, useRef, useState } from "react";
import { getEarningsSummary, type MarketKey } from "./api";

const MONTH_TO_QUARTER: Record<string, { q: number; fyOffset: number }> = {
  jun: { q: 1, fyOffset: 0 },
  june: { q: 1, fyOffset: 0 },
  sep: { q: 2, fyOffset: 0 },
  september: { q: 2, fyOffset: 0 },
  dec: { q: 3, fyOffset: 0 },
  december: { q: 3, fyOffset: 0 },
  mar: { q: 4, fyOffset: -1 },
  march: { q: 4, fyOffset: -1 },
};

export type QuarterInfo = {
  raw: string;
  q: number;
  fy: number;
  short: string;
  rank: number;
};

export function parsePeriod(period: string | null | undefined): QuarterInfo | null {
  if (!period) return null;
  const m = period.trim().match(/([A-Za-z]+)\s+(\d{4})/);
  if (!m) return null;
  const monthKey = m[1].toLowerCase();
  const year = parseInt(m[2], 10);
  const map = MONTH_TO_QUARTER[monthKey];
  if (!map) return null;
  const fy = year + map.fyOffset;
  return {
    raw: period,
    q: map.q,
    fy,
    short: `Q${map.q} FY${String(fy).slice(-2)}`,
    rank: fy * 4 + map.q,
  };
}

const earningsCache = new Map<string, string | null>();
const inFlight = new Map<string, Promise<string | null>>();

async function fetchOne(symbol: string, market: MarketKey): Promise<string | null> {
  const key = `${market}::${symbol}`;
  if (earningsCache.has(key)) return earningsCache.get(key) ?? null;
  const existing = inFlight.get(key);
  if (existing) return existing;
  const p = (async () => {
    try {
      const r = await getEarningsSummary(symbol, market);
      const period = r?.quarterly_results?.[0]?.period || null;
      earningsCache.set(key, period);
      return period;
    } catch {
      earningsCache.set(key, null);
      return null;
    } finally {
      inFlight.delete(key);
    }
  })();
  inFlight.set(key, p);
  return p;
}

export function useLatestQuarters(
  symbols: string[],
  market: MarketKey,
  options: { concurrency?: number; enabled?: boolean } = {},
): Record<string, QuarterInfo | null> {
  const concurrency = options.concurrency ?? 4;
  const enabled = options.enabled !== false;
  const [periods, setPeriods] = useState<Record<string, string | null>>({});
  const symbolKeyRef = useRef<string>("");

  useEffect(() => {
    if (!enabled) return;
    const list = Array.from(new Set(symbols.map((s) => s.toUpperCase()))).filter(Boolean);
    const sig = `${market}::${list.join("|")}`;
    if (symbolKeyRef.current === sig) return;
    symbolKeyRef.current = sig;

    let aborted = false;
    setPeriods((prev) => {
      const next: Record<string, string | null> = {};
      list.forEach((s) => {
        const cached = earningsCache.get(`${market}::${s}`);
        next[s] = cached !== undefined ? cached : prev[s] ?? null;
      });
      return next;
    });

    const queue = list.filter((s) => !earningsCache.has(`${market}::${s}`));
    let cursor = 0;
    const run = async () => {
      while (!aborted && cursor < queue.length) {
        const idx = cursor++;
        const sym = queue[idx];
        const period = await fetchOne(sym, market);
        if (aborted) return;
        setPeriods((prev) => ({ ...prev, [sym]: period }));
      }
    };
    const workers = Array.from({ length: Math.min(concurrency, queue.length) }, () => run());
    void Promise.all(workers);

    return () => {
      aborted = true;
    };
  }, [symbols, market, concurrency, enabled]);

  const out: Record<string, QuarterInfo | null> = {};
  for (const sym of Object.keys(periods)) {
    out[sym] = parsePeriod(periods[sym]);
  }
  return out;
}
