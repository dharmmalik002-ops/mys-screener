import { useSyncExternalStore } from "react";
import { DEFAULT_CHART_COLORS } from "../../lib/chartDefaults";

/* Chart options shared by every chart on the Course page — the same choices
   the site's main chart offers (Candles / Bars / HLC, EMA10 / EMA20 / EMA50 /
   SMA200), plus a swing chart: the zigzag joining swing highs and lows.

   First use starts from the main chart's own saved preferences
   (`mr-malik-chart-preferences:v2:india`, written by App.tsx), so the course
   draws the way you already chose to see charts, in your colours. After that
   the course keeps its own copy, and one toolbar change redraws every chart on
   the page (a tiny external store, not per-chart state). */

export type CourseChartStyle = "candles" | "bars" | "hlc";
export type MaKey = "ema10" | "ema20" | "ema50" | "ema200";
export type CourseChartOptions = {
  style: CourseChartStyle;
  mas: MaKey[];
  swings: boolean;
};
export type CourseChartColors = { up: string; down: string; ma: Record<MaKey, string> };

export const MA_DEFS: { key: MaKey; label: string; span: number; kind: "ema" | "sma" }[] = [
  { key: "ema10", label: "EMA10", span: 10, kind: "ema" },
  { key: "ema20", label: "EMA20", span: 20, kind: "ema" },
  { key: "ema50", label: "EMA50", span: 50, kind: "ema" },
  // The main chart labels its fourth line SMA200 and computes it as a simple average.
  { key: "ema200", label: "SMA200", span: 200, kind: "sma" },
];

const KEY = "mr-malik-course-chart:v1";
const SITE_KEY = "mr-malik-chart-preferences:v2:india";
const MA_KEYS = new Set<MaKey>(MA_DEFS.map((m) => m.key));

type SitePrefs = { chartStyle?: string; indicatorKeys?: string[]; chartColors?: Record<string, string> };

function readSite(): SitePrefs {
  try {
    return (JSON.parse(localStorage.getItem(SITE_KEY) || "{}") as SitePrefs) || {};
  } catch {
    return {};
  }
}

function normStyle(v: unknown): CourseChartStyle {
  return v === "bars" || v === "hlc" ? v : "candles";
}

function initial(): CourseChartOptions {
  try {
    const own = JSON.parse(localStorage.getItem(KEY) || "null") as Partial<CourseChartOptions> | null;
    if (own) {
      return {
        style: normStyle(own.style),
        mas: (own.mas ?? []).filter((m): m is MaKey => MA_KEYS.has(m as MaKey)),
        swings: own.swings === true,
      };
    }
  } catch {
    /* fall through to the site's own chart preferences */
  }
  const site = readSite();
  const mas = (site.indicatorKeys ?? ["ema20", "ema50"]).filter((m): m is MaKey => MA_KEYS.has(m as MaKey));
  return { style: normStyle(site.chartStyle), mas, swings: false };
}

export function courseChartColors(): CourseChartColors {
  const c = { ...DEFAULT_CHART_COLORS, ...(readSite().chartColors ?? {}) } as typeof DEFAULT_CHART_COLORS;
  return { up: c.candleUp, down: c.candleDown, ma: { ema10: c.ema10, ema20: c.ema20, ema50: c.ema50, ema200: c.ema200 } };
}

let current: CourseChartOptions | null = null;
const listeners = new Set<() => void>();

function get(): CourseChartOptions {
  if (!current) current = initial();
  return current;
}

export function setCourseChartOptions(next: CourseChartOptions) {
  current = next;
  try {
    localStorage.setItem(KEY, JSON.stringify(next));
  } catch {
    /* per-viewer convenience only */
  }
  listeners.forEach((fn) => fn());
}

function subscribe(fn: () => void) {
  listeners.add(fn);
  return () => listeners.delete(fn);
}

export function useCourseChartOptions(): CourseChartOptions {
  return useSyncExternalStore(subscribe, get, get);
}

export function CourseChartToolbar() {
  const opts = useCourseChartOptions();
  const set = (patch: Partial<CourseChartOptions>) => setCourseChartOptions({ ...opts, ...patch });
  const toggleMa = (key: MaKey) => set({ mas: opts.mas.includes(key) ? opts.mas.filter((m) => m !== key) : [...opts.mas, key] });
  const colors = courseChartColors();
  return (
    <div className="course-chart-toolbar" role="group" aria-label="Chart options">
      <span className="course-chart-toolbar-group">
        {(["candles", "bars", "hlc"] as const).map((s) => (
          <button key={s} type="button" className={`lookalike-filter${opts.style === s ? " is-active" : ""}`} aria-pressed={opts.style === s} onClick={() => set({ style: s })}>
            {s === "candles" ? "Candles" : s === "bars" ? "Bars" : "HLC"}
          </button>
        ))}
      </span>
      <span className="course-chart-toolbar-group">
        {MA_DEFS.map((m) => (
          <button
            key={m.key}
            type="button"
            className={`lookalike-filter${opts.mas.includes(m.key) ? " is-active" : ""}`}
            aria-pressed={opts.mas.includes(m.key)}
            onClick={() => toggleMa(m.key)}
          >
            <i className="course-ma-swatch" style={{ background: colors.ma[m.key] }} aria-hidden />
            {m.label}
          </button>
        ))}
      </span>
      <button
        type="button"
        className={`lookalike-filter${opts.swings ? " is-active" : ""}`}
        aria-pressed={opts.swings}
        onClick={() => set({ swings: !opts.swings })}
        title="Swing chart: a line joining each swing high and swing low (a bar higher or lower than the 5 sessions either side)"
      >
        Swing chart
      </button>
    </div>
  );
}
