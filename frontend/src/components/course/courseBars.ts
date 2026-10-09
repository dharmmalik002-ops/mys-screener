import { getCourseBars, type StudyBar } from "../../lib/api";

/* Daily bars for a past window, through /api/course/bars (which keeps a disk
   cache on the Space). One request per (symbol, window) per page load: every
   chart showing the same window shares it, and a failure is forgotten so the
   next attempt can retry. */

const cache = new Map<string, Promise<StudyBar[]>>();
const DAY_MS = 86_400_000;

export const isoShift = (iso: string, days: number) => new Date(Date.parse(`${iso}T00:00:00Z`) + days * DAY_MS).toISOString().slice(0, 10);
export const toTime = (iso: string) => Math.floor(Date.parse(`${iso}T00:00:00Z`) / 1000);

export function loadBars(symbol: string, start: string, end: string): Promise<StudyBar[]> {
  const key = `${symbol}|${start}|${end}`;
  let hit = cache.get(key);
  if (!hit) {
    hit = getCourseBars(symbol, start, end).then((r) => r?.bars ?? []);
    hit.catch(() => cache.delete(key));
    cache.set(key, hit);
  }
  return hit;
}

/** Index of the last bar on or before `t`, or -1 when every bar is later. */
export function barAtOrBefore(bars: StudyBar[], t: number) {
  let found = -1;
  for (let i = 0; i < bars.length; i += 1) {
    if (bars[i].time <= t) found = i;
    else break;
  }
  return found;
}
