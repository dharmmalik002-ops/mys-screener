/* The Results page's rules: filters, sort orders, date presets and number
   formats, kept apart from the component so they can be tested. Figures are
   standalone, in crore, as BSE files them (backend services/results_feed.py). */

import type { QuarterlyResultEntry, ResultMetricKey, ResultsCapBucket } from "./api";

export type ResultsSort = "latest" | ResultMetricKey;
export type ResultsScope = "all" | "watchlist";

export type ResultsFilters = {
  caps: ResultsCapBucket[]; // empty = every size
  sort: ResultsSort;
  scope: ResultsScope;
  sectors: string[]; // empty = every sector
  search: string;
};

export const DEFAULT_FILTERS: ResultsFilters = { caps: [], sort: "latest", scope: "all", sectors: [], search: "" };

export const CAP_OPTIONS: { key: ResultsCapBucket; label: string; hint: string }[] = [
  { key: "large", label: "Large cap", hint: "Top 100 by market cap" },
  { key: "mid", label: "Mid cap", hint: "101st to 250th" },
  { key: "small", label: "Small cap", hint: "251st to 500th" },
  { key: "micro", label: "Micro cap", hint: "501st onward (over ₹500 cr)" },
];

export const SORT_OPTIONS: { key: ResultsSort; label: string }[] = [
  { key: "latest", label: "Latest" },
  { key: "sales", label: "Sales YoY%" },
  { key: "operating_profit", label: "Operating Profit YoY%" },
  { key: "net_profit", label: "Net Profit YoY%" },
  { key: "eps", label: "EPS YoY%" },
];

type Filterable = {
  symbol: string;
  name: string;
  cap_bucket: ResultsCapBucket | null;
  sector: string | null;
};

export function filterResults<T extends Filterable>(items: T[], filters: ResultsFilters, watchlist: ReadonlySet<string>): T[] {
  const query = filters.search.trim().toLowerCase();
  const caps = new Set(filters.caps);
  const sectors = new Set(filters.sectors);
  return items.filter((item) => {
    if (caps.size && !(item.cap_bucket && caps.has(item.cap_bucket))) return false;
    if (sectors.size && !(item.sector && sectors.has(item.sector))) return false;
    if (filters.scope === "watchlist" && !watchlist.has(item.symbol.toUpperCase())) return false;
    if (query && !item.symbol.toLowerCase().includes(query) && !item.name.toLowerCase().includes(query)) return false;
    return true;
  });
}

/* Latest: newest declaration first, larger companies first within a minute.
   A growth sort is highest first; a result whose growth cannot be measured
   (a loss a year ago, or no year-ago quarter) goes to the end, not the top. */
export function sortResults(items: QuarterlyResultEntry[], sort: ResultsSort): QuarterlyResultEntry[] {
  const copy = [...items];
  if (sort === "latest") {
    return copy.sort((a, b) =>
      a.declared_at === b.declared_at
        ? (b.market_cap_crore ?? 0) - (a.market_cap_crore ?? 0)
        : a.declared_at < b.declared_at ? 1 : -1,
    );
  }
  return copy.sort((a, b) => {
    const x = a.metrics[sort]?.yoy_pct;
    const y = b.metrics[sort]?.yoy_pct;
    if (x == null && y == null) return (b.market_cap_crore ?? 0) - (a.market_cap_crore ?? 0);
    if (x == null) return 1;
    if (y == null) return -1;
    return y - x;
  });
}

export function sectorCounts(items: { sector: string | null }[]): [string, number][] {
  const counts = new Map<string, number>();
  for (const item of items) {
    if (item.sector) counts.set(item.sector, (counts.get(item.sector) ?? 0) + 1);
  }
  return [...counts.entries()].sort((a, b) => b[1] - a[1] || a[0].localeCompare(b[0]));
}

export function groupByDay<T extends { declared_date: string }>(items: T[]): { date: string; items: T[] }[] {
  const groups: { date: string; items: T[] }[] = [];
  for (const item of items) {
    const last = groups[groups.length - 1];
    if (last && last.date === item.declared_date) last.items.push(item);
    else groups.push({ date: item.declared_date, items: [item] });
  }
  return groups;
}

/* ── dates (ISO yyyy-mm-dd, IST) ─────────────────────────────────────────── */

export function todayIst(now: Date = new Date()): string {
  return new Date(now.getTime() + 330 * 60_000).toISOString().slice(0, 10);
}

function shiftDays(iso: string, days: number): string {
  const date = new Date(`${iso}T00:00:00Z`);
  date.setUTCDate(date.getUTCDate() + days);
  return date.toISOString().slice(0, 10);
}

/* First day of the calendar quarter: the season of results running then
   (1 Oct for the September quarter's results). */
export function seasonStart(iso: string): string {
  const [year, month] = iso.split("-").map(Number);
  const first = Math.floor((month - 1) / 3) * 3 + 1;
  return `${year}-${String(first).padStart(2, "0")}-01`;
}

export type RangePreset = { key: string; label: string; from: string; to: string };

export function rangePresets(today: string): RangePreset[] {
  const season = seasonStart(today);
  const lastSeasonEnd = shiftDays(season, -1);
  return [
    { key: "today", label: "Today", from: today, to: today },
    { key: "week", label: "Last 7 days", from: shiftDays(today, -6), to: today },
    { key: "season", label: "This season", from: season, to: today },
    { key: "last-season", label: "Last season", from: seasonStart(lastSeasonEnd), to: lastSeasonEnd },
  ];
}

const MONTHS = ["Jan", "Feb", "Mar", "Apr", "May", "Jun", "Jul", "Aug", "Sep", "Oct", "Nov", "Dec"];
const WEEKDAYS = ["Sunday", "Monday", "Tuesday", "Wednesday", "Thursday", "Friday", "Saturday"];

export function formatDay(iso: string, withWeekday = false): string {
  const [year, month, day] = iso.slice(0, 10).split("-").map(Number);
  if (!year || !month || !day) return iso;
  const text = `${day} ${MONTHS[month - 1]} ${year}`;
  if (!withWeekday) return text;
  return `${WEEKDAYS[new Date(Date.UTC(year, month - 1, day)).getUTCDay()]}, ${text}`;
}

/* "2026-10-09T17:42" -> "9 Oct 2026 · 5:42 PM" (BSE filing time, IST). */
export function formatDeclared(iso: string): string {
  const time = iso.slice(11, 16);
  if (!/^\d\d:\d\d$/.test(time)) return formatDay(iso);
  const [hours, minutes] = time.split(":").map(Number);
  const suffix = hours >= 12 ? "PM" : "AM";
  const hour12 = hours % 12 === 0 ? 12 : hours % 12;
  return `${formatDay(iso)} · ${hour12}:${String(minutes).padStart(2, "0")} ${suffix}`;
}

/* ── numbers ─────────────────────────────────────────────────────────────── */

const INR0 = new Intl.NumberFormat("en-IN", { maximumFractionDigits: 0 });
const INR2 = new Intl.NumberFormat("en-IN", { minimumFractionDigits: 2, maximumFractionDigits: 2 });

export function formatCrore(value: number | null | undefined): string {
  if (value == null || !Number.isFinite(value)) return "—";
  return Math.abs(value) >= 100 ? INR0.format(value) : INR2.format(value);
}

export function formatEps(value: number | null | undefined): string {
  return value == null || !Number.isFinite(value) ? "—" : INR2.format(value);
}

export function formatPct(value: number | null | undefined): string {
  return value == null || !Number.isFinite(value) ? "—" : `${INR2.format(value)}%`;
}

export function formatMarketCap(crore: number | null | undefined): string {
  if (crore == null || !Number.isFinite(crore) || crore <= 0) return "—";
  return crore >= 100_000 ? `₹${INR2.format(crore / 100_000)} L Cr.` : `₹${INR0.format(crore)} Cr.`;
}

export function formatRatio(value: number | null | undefined): string {
  return value == null || !Number.isFinite(value) ? "—" : INR2.format(value);
}

export function growthTone(value: number | null | undefined): "up" | "down" | "flat" {
  if (value == null || !Number.isFinite(value) || value === 0) return "flat";
  return value > 0 ? "up" : "down";
}
