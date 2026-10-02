import { useEffect, useMemo, useRef, useState } from "react";

import "./PnlYearGrid.css";

/**
 * A year of realised P&L at a glance: one square per trading day (Mon–Fri),
 * one column per week, green for a profitable day and red for a losing one,
 * darker the bigger the day. Three readings of the same year:
 *
 *   Daily       — each session on its own
 *   Weekly      — each week as one bar, so a week of small wins reads as one
 *   Cumulative  — the running total since 1 January, so a drawdown shows as a
 *                 red stretch however the individual days fell
 *
 * Colour depth is scaled to the year's own 90th-percentile day rather than
 * its single biggest one, or one outsized day would wash every other cell out.
 */

export type PnlYearEntry<T> = {
  /** Exit date, `YYYY-MM-DD` (anything after the date is ignored). */
  date: string;
  pnl: number;
  item: T;
};

type View = "daily" | "weekly" | "cumulative";

type Cell<T> = {
  key: string;
  date: Date;
  inYear: boolean;
  future: boolean;
  pnl: number | null;
  items: T[];
  /** Running total since 1 January, through this day. */
  running: number;
};

const VIEWS: Array<{ key: View; label: string }> = [
  { key: "daily", label: "Daily" },
  { key: "weekly", label: "Weekly" },
  { key: "cumulative", label: "Cumulative" },
];

const WEEKDAYS = ["Mon", "", "Wed", "", "Fri"];
const MONTHS = ["Jan", "Feb", "Mar", "Apr", "May", "Jun", "Jul", "Aug", "Sep", "Oct", "Nov", "Dec"];

function dayKey(d: Date) {
  return `${d.getFullYear()}-${String(d.getMonth() + 1).padStart(2, "0")}-${String(d.getDate()).padStart(2, "0")}`;
}

function parseDay(value: string): Date | null {
  const match = /^(\d{4})-(\d{2})-(\d{2})/.exec(value || "");
  if (!match) return null;
  return new Date(Number(match[1]), Number(match[2]) - 1, Number(match[3]));
}

/** Monday on or before `d`. */
function weekStart(d: Date) {
  const offset = (d.getDay() + 6) % 7;
  return new Date(d.getFullYear(), d.getMonth(), d.getDate() - offset);
}

function addDays(d: Date, n: number) {
  return new Date(d.getFullYear(), d.getMonth(), d.getDate() + n);
}

function percentile(values: number[], q: number) {
  if (values.length === 0) return 0;
  const sorted = [...values].sort((a, b) => a - b);
  const index = Math.min(sorted.length - 1, Math.max(0, Math.round(q * (sorted.length - 1))));
  return sorted[index];
}

/** 0 = no data, 1..4 = depth of colour. */
function level(value: number, scale: number) {
  if (value === 0 || scale <= 0) return 1;
  const ratio = Math.abs(value) / scale;
  if (ratio >= 1) return 4;
  if (ratio >= 0.5) return 3;
  if (ratio >= 0.2) return 2;
  return 1;
}

function formatRupees(n: number) {
  const abs = Math.abs(n).toLocaleString("en-IN", { maximumFractionDigits: 0 });
  return `${n >= 0 ? "+" : "−"}₹${abs}`;
}

type PnlYearGridProps<T> = {
  entries: Array<PnlYearEntry<T>>;
  /** Called with the cell's label and the trades that closed in it. */
  onSelectCell?: (label: string, items: T[]) => void;
};

export function PnlYearGrid<T>({ entries, onSelectCell }: PnlYearGridProps<T>) {
  const today = useMemo(() => new Date(), []);

  const byDay = useMemo(() => {
    const map = new Map<string, { pnl: number; items: T[] }>();
    for (const entry of entries) {
      const date = parseDay(entry.date);
      if (!date || !Number.isFinite(entry.pnl)) continue;
      // Weekend sessions are rare (Budget Saturday) and the grid has no row
      // for them; they are filed under the Friday before rather than dropped.
      const dow = date.getDay();
      const filed = dow === 6 ? addDays(date, -1) : dow === 0 ? addDays(date, -2) : date;
      const key = dayKey(filed);
      const row = map.get(key) ?? { pnl: 0, items: [] };
      row.pnl += entry.pnl;
      row.items.push(entry.item);
      map.set(key, row);
    }
    return map;
  }, [entries]);

  const years = useMemo(() => {
    const set = new Set<number>([today.getFullYear()]);
    for (const key of byDay.keys()) set.add(Number(key.slice(0, 4)));
    return [...set].sort((a, b) => a - b);
  }, [byDay, today]);

  const latestYearWithTrades = useMemo(() => {
    let latest: number | null = null;
    for (const key of byDay.keys()) {
      const y = Number(key.slice(0, 4));
      if (latest === null || y > latest) latest = y;
    }
    return latest ?? today.getFullYear();
  }, [byDay, today]);

  const [year, setYear] = useState<number>(latestYearWithTrades);
  const [view, setView] = useState<View>("daily");

  const { weeks, monthStarts, stats } = useMemo(() => {
    const jan1 = new Date(year, 0, 1);
    const dec31 = new Date(year, 11, 31);
    const first = weekStart(jan1);
    const out: Array<Cell<T>[]> = [];
    const weekMs = 7 * 24 * 60 * 60 * 1000;
    // Each month's label sits over the week that contains its 1st.
    const starts = MONTHS.map((label, m) => ({
      index: Math.round((weekStart(new Date(year, m, 1)).getTime() - first.getTime()) / weekMs),
      label,
    }));
    let running = 0;
    const dailyValues: number[] = [];
    let green = 0;
    let red = 0;
    let best: { key: string; pnl: number } | null = null;
    let worst: { key: string; pnl: number } | null = null;
    let total = 0;

    for (let monday = first; monday <= dec31; monday = addDays(monday, 7)) {
      const column: Cell<T>[] = [];
      for (let i = 0; i < 5; i++) {
        const date = addDays(monday, i);
        const key = dayKey(date);
        const inYear = date.getFullYear() === year;
        const future = date > today;
        const row = inYear ? byDay.get(key) : undefined;
        if (row) {
          dailyValues.push(Math.abs(row.pnl));
          total += row.pnl;
          if (row.pnl > 0) green++;
          else if (row.pnl < 0) red++;
          if (!best || row.pnl > best.pnl) best = { key, pnl: row.pnl };
          if (!worst || row.pnl < worst.pnl) worst = { key, pnl: row.pnl };
        }
        if (inYear && row) running += row.pnl;
        column.push({
          key,
          date,
          inYear,
          future,
          pnl: row ? row.pnl : null,
          items: row ? row.items : [],
          running,
        });
      }
      out.push(column);
    }
    return {
      weeks: out,
      monthStarts: starts,
      stats: {
        dailyScale: percentile(dailyValues, 0.9),
        green,
        red,
        best,
        worst,
        total,
      },
    };
  }, [byDay, today, year]);

  const weeklyTotals = useMemo(
    () =>
      weeks.map((column) => {
        const traded = column.filter((cell) => cell.pnl !== null);
        return {
          pnl: traded.length ? traded.reduce((sum, cell) => sum + (cell.pnl ?? 0), 0) : null,
          items: traded.flatMap((cell) => cell.items),
          from: column[0].date,
          inYear: column.some((cell) => cell.inYear),
          future: column[0].date > today,
        };
      }),
    [today, weeks],
  );

  const weeklyScale = useMemo(
    () => percentile(weeklyTotals.filter((w) => w.pnl !== null).map((w) => Math.abs(w.pnl ?? 0)), 0.9),
    [weeklyTotals],
  );
  const cumulativeScale = useMemo(() => {
    let max = 0;
    for (const column of weeks) for (const cell of column) max = Math.max(max, Math.abs(cell.running));
    return max;
  }, [weeks]);

  // On a narrow screen the grid scrolls sideways; open the current year at
  // its latest weeks, which is the part anyone opens this to look at.
  const scrollRef = useRef<HTMLDivElement | null>(null);
  useEffect(() => {
    const node = scrollRef.current;
    if (!node) return;
    node.scrollLeft = year === today.getFullYear() ? node.scrollWidth : 0;
  }, [today, year]);

  const yearIndex = years.indexOf(year);
  const tradedDays = stats.green + stats.red;

  const cellClass = (value: number | null, scale: number, extra = "") => {
    if (value === null) return `pyg-cell ${extra}`.trim();
    const tone = value > 0 ? "pos" : value < 0 ? "neg" : "flat";
    return `pyg-cell is-${tone} l${level(value, scale)} ${extra}`.trim();
  };

  return (
    <div className="pyg-root">
      <div className="pyg-toolbar">
        <div className="pyg-year">
          <button
            type="button"
            className="pyg-nav"
            onClick={() => setYear(years[yearIndex - 1])}
            disabled={yearIndex <= 0}
            aria-label="Previous year"
          >
            ‹
          </button>
          <strong>{year}</strong>
          <button
            type="button"
            className="pyg-nav"
            onClick={() => setYear(years[yearIndex + 1])}
            disabled={yearIndex < 0 || yearIndex >= years.length - 1}
            aria-label="Next year"
          >
            ›
          </button>
        </div>
        <div className="pyg-seg" role="tablist" aria-label="Heatmap reading">
          {VIEWS.map((option) => (
            <button
              key={option.key}
              type="button"
              role="tab"
              aria-selected={view === option.key}
              className={`pyg-seg-btn${view === option.key ? " is-active" : ""}`}
              onClick={() => setView(option.key)}
            >
              {option.label}
            </button>
          ))}
        </div>
      </div>

      {tradedDays === 0 ? (
        <div className="pyg-empty">No closed trades in {year}.</div>
      ) : null}

      <div className="pyg-scroll" ref={scrollRef}>
        <div className="pyg-frame" style={{ ["--pyg-weeks" as string]: weeks.length }}>
          <div className="pyg-weekdays" aria-hidden="true">
            {WEEKDAYS.map((label, i) => (
              <span key={i}>{label}</span>
            ))}
          </div>
          <div className="pyg-grid">
            {view === "weekly"
              ? [
                  // Bars have no intrinsic height; five invisible squares in the
                  // first column give the rows the same height as the daily grid.
                  ...[0, 1, 2, 3, 4].map((d) => (
                    <span key={`spacer-${d}`} className="pyg-cell pyg-spacer" style={{ gridColumn: 1, gridRow: d + 1 }} />
                  )),
                  ...weeklyTotals.map((week, w) => {
                  const label = `Week of ${dayKey(week.from)}`;
                  const cls = cellClass(week.pnl, weeklyScale, `pyg-bar${week.future ? " is-future" : ""}${!week.inYear ? " is-out" : ""}`);
                  const title = week.pnl === null ? label : `${label}: ${formatRupees(week.pnl)} · ${week.items.length} trade${week.items.length === 1 ? "" : "s"}`;
                  return week.pnl !== null && onSelectCell ? (
                    <button
                      key={w}
                      type="button"
                      className={cls}
                      style={{ gridColumn: w + 1 }}
                      title={title}
                      onClick={() => onSelectCell(label, week.items)}
                    />
                  ) : (
                    <span key={w} className={cls} style={{ gridColumn: w + 1 }} title={title} />
                  );
                  }),
                ]
              : weeks.map((column, w) =>
                  column.map((cell, d) => {
                    const value =
                      view === "cumulative"
                        ? cell.inYear && !cell.future
                          ? cell.running
                          : null
                        : cell.pnl;
                    const scale = view === "cumulative" ? cumulativeScale : stats.dailyScale;
                    const extra = [!cell.inYear ? "is-out" : "", cell.future ? "is-future" : ""].filter(Boolean).join(" ");
                    // A cumulative cell before the first trade is a true zero,
                    // not "no trades" — keep it neutral rather than tinted.
                    const shown = view === "cumulative" && value === 0 ? null : value;
                    const cls = cellClass(cell.inYear ? shown : null, scale, extra);
                    const title =
                      view === "cumulative"
                        ? `${cell.key}: ${formatRupees(cell.running)} so far this year`
                        : cell.pnl === null
                          ? cell.key
                          : `${cell.key}: ${formatRupees(cell.pnl)} · ${cell.items.length} trade${cell.items.length === 1 ? "" : "s"}`;
                    const clickable = cell.pnl !== null && cell.inYear && onSelectCell;
                    const style = { gridColumn: w + 1, gridRow: d + 1 };
                    return clickable ? (
                      <button
                        key={cell.key}
                        type="button"
                        className={cls}
                        style={style}
                        title={title}
                        onClick={() => onSelectCell(cell.key, cell.items)}
                      />
                    ) : (
                      <span key={cell.key} className={cls} style={style} title={title} />
                    );
                  }),
                )}
          </div>
          <div className="pyg-months" aria-hidden="true">
            {monthStarts.map((m) => (
              <span key={m.label} style={{ gridColumn: m.index + 1 }}>
                {m.label}
              </span>
            ))}
          </div>
        </div>
      </div>

      <div className="pyg-footer">
        <div className="pyg-stats">
          <span>
            Year <strong className={stats.total >= 0 ? "pos" : "neg"}>{formatRupees(stats.total)}</strong>
          </span>
          <span>
            <strong className="pos">{stats.green}</strong> green · <strong className="neg">{stats.red}</strong> red days
          </span>
          {stats.best && stats.best.pnl > 0 ? (
            <span>
              Best <strong className="pos">{formatRupees(stats.best.pnl)}</strong> {stats.best.key.slice(5)}
            </span>
          ) : null}
          {stats.worst && stats.worst.pnl < 0 ? (
            <span>
              Worst <strong className="neg">{formatRupees(stats.worst.pnl)}</strong> {stats.worst.key.slice(5)}
            </span>
          ) : null}
        </div>
        <div className="pyg-legend" aria-hidden="true">
          <span>Loss</span>
          <i className="pyg-cell is-neg l4" />
          <i className="pyg-cell is-neg l2" />
          <i className="pyg-cell" />
          <i className="pyg-cell is-pos l2" />
          <i className="pyg-cell is-pos l4" />
          <span>Profit</span>
        </div>
      </div>
    </div>
  );
}
