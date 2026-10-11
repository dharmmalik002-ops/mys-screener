import { useCallback, useEffect, useMemo, useState } from "react";
import { CalendarDays, ExternalLink, RefreshCw, Search } from "lucide-react";

import {
  getQuarterlyResults,
  getUpcomingResults,
  type ResultsBasis,
  type QuarterlyResultsResponse,
  type ResultMetricKey,
  type UpcomingResultsResponse,
} from "../lib/api";
import {
  CAP_OPTIONS,
  DEFAULT_FILTERS,
  SORT_OPTIONS,
  filterResults,
  formatCrore,
  formatDay,
  formatDeclared,
  formatEps,
  formatMarketCap,
  formatPct,
  formatRatio,
  groupByDay,
  growthTone,
  rangePresets,
  sectorCounts,
  seasonStart,
  showBasis,
  showUpcomingBasis,
  sortResults,
  type ShownResult,
  type ShownUpcoming,
  todayIst,
  type ResultsFilters,
} from "../lib/results";

import "./ResultsPanel.css";

/* The Results page: each company's latest quarter, on the day it was
   declared — sales, operating profit, net profit and EPS with YoY / QoQ
   growth, as in an exchange results tracker. Figures are STANDALONE from
   BSE's filings; the evening job (7 PM IST, then 9:30 PM and 11:45 PM for
   late filers) adds the day's results with the minute each was filed. */

type ResultsView = "quarterly" | "upcoming";

type ResultsPanelProps = {
  watchlistSymbols: string[];
  onOpenChart: (symbol: string, symbols: string[]) => void;
};

const STORAGE_KEY = "mr-malik-results:v1";
const PAGE_SIZE = 30;
const SECTORS_SHOWN = 6;
const REFRESH_MS = 5 * 60_000;

const METRIC_ROWS: { key: ResultMetricKey; label: string }[] = [
  { key: "sales", label: "Sales" },
  { key: "operating_profit", label: "Operating Profit" },
  { key: "net_profit", label: "Net Profit" },
  { key: "eps", label: "EPS" },
];

type Saved = { view: ResultsView; filters: ResultsFilters; basis: ResultsBasis };

function readSaved(): Saved {
  try {
    const raw = JSON.parse(window.localStorage.getItem(STORAGE_KEY) ?? "null");
    if (raw && typeof raw === "object") {
      return {
        view: raw.view === "upcoming" ? "upcoming" : "quarterly",
        filters: { ...DEFAULT_FILTERS, ...(raw.filters ?? {}), search: "" },
        basis: raw.basis === "standalone" ? "standalone" : "consolidated",
      };
    }
  } catch {
    /* private window or blocked storage: defaults */
  }
  return { view: "quarterly", filters: DEFAULT_FILTERS, basis: "consolidated" };
}

function toggle<T>(list: T[], value: T): T[] {
  return list.includes(value) ? list.filter((item) => item !== value) : [...list, value];
}

function Growth({ value }: { value: number | null }) {
  return <span className={`results-growth is-${growthTone(value)}`}>{formatPct(value)}</span>;
}

const BASIS_LABEL: Record<ResultsBasis, string> = { consolidated: "Consolidated", standalone: "Standalone" };

function BasisTag({ shown, wanted }: { shown: ResultsBasis | null; wanted: ResultsBasis }) {
  if (!shown) return null;
  const title =
    shown === wanted
      ? undefined
      : shown === "standalone"
        ? "No consolidated filing for this quarter (no subsidiaries, or not on NSE yet): standalone figures shown"
        : "Filed consolidated on NSE only: consolidated figures shown";
  return (
    <span className={`results-basis${shown === wanted ? "" : " is-fallback"}`} title={title}>
      {BASIS_LABEL[shown]}
    </span>
  );
}

function ResultCard({ item, basis, onOpen }: { item: ShownResult; basis: ResultsBasis; onOpen: (symbol: string) => void }) {
  return (
    <article className="results-card">
      <header className="results-card-head">
        <div className="results-card-title">
          <button type="button" className="results-name" onClick={() => onOpen(item.symbol)} title="Open chart">
            {item.name}
          </button>
          <span className="results-symbol">{item.symbol}</span>
          <BasisTag shown={item.shown_basis} wanted={basis} />
          <span className="results-declared" title="When the result was filed on BSE (IST)">
            {formatDeclared(item.declared_at)}
          </span>
        </div>
        <dl className="results-valuation">
          <div><dt>M Cap</dt><dd>{formatMarketCap(item.market_cap_crore)}</dd></div>
          <div><dt title={`Market cap over the last four quarters' ${item.shown_basis} net profit`}>PE</dt><dd>{formatRatio(item.pe)}</dd></div>
          <div><dt title="PE over the growth of trailing net profit on the year before">PEG</dt><dd>{formatRatio(item.peg)}</dd></div>
        </dl>
      </header>
      <div className="results-table-wrap">
        <table className="results-table">
          <thead>
            <tr>
              <th scope="col">(In Cr.)</th>
              <th scope="col">YoY Growth</th>
              <th scope="col">QoQ Growth</th>
              <th scope="col">{item.period}</th>
              <th scope="col">{item.prev_period ?? "Prev Q"}</th>
              <th scope="col">{item.year_ago_period ?? "Year ago"}</th>
            </tr>
          </thead>
          <tbody>
            {METRIC_ROWS.map(({ key, label }) => {
              const metric = item.metrics[key];
              const format = key === "eps" ? formatEps : formatCrore;
              return (
                <tr key={key}>
                  <th scope="row">{label}</th>
                  <td><Growth value={metric?.yoy_pct ?? null} /></td>
                  <td><Growth value={metric?.qoq_pct ?? null} /></td>
                  <td>{format(metric?.value)}</td>
                  <td>{format(metric?.prev)}</td>
                  <td>{format(metric?.year_ago)}</td>
                </tr>
              );
            })}
          </tbody>
        </table>
      </div>
      <footer className="results-card-foot">
        {item.sector ? <span className="results-sector">{item.sub_sector && item.sub_sector !== "Unclassified" ? item.sub_sector : item.sector}</span> : <span />}
        {item.result_url ? (
          <a href={item.result_url} target="_blank" rel="noreferrer">
            Result on BSE <ExternalLink size={12} aria-hidden />
          </a>
        ) : null}
      </footer>
    </article>
  );
}

function UpcomingDay({ date, items, basis, onOpen }: { date: string; items: ShownUpcoming[]; basis: ResultsBasis; onOpen: (symbol: string) => void }) {
  return (
    <section className="results-day">
      <h3 className="results-day-head">
        {formatDay(date, true)} <span>{items.length} {items.length === 1 ? "company" : "companies"}</span>
      </h3>
      <div className="results-table-wrap results-upcoming-wrap">
        <table className="results-table results-upcoming">
          <thead>
            <tr>
              <th scope="col">Company</th>
              <th scope="col">M Cap</th>
              <th scope="col">Last quarter</th>
              <th scope="col">Sales YoY</th>
              <th scope="col">Profit YoY</th>
              <th scope="col">PE</th>
            </tr>
          </thead>
          <tbody>
            {items.map((item) => (
              <tr key={item.symbol}>
                <th scope="row">
                  <button type="button" className="results-name" onClick={() => onOpen(item.symbol)} title="Open chart">
                    {item.name}
                  </button>
                  <span className="results-symbol">{item.symbol}</span>
                  <BasisTag shown={item.shown_basis} wanted={basis} />
                </th>
                <td>{formatMarketCap(item.market_cap_crore)}</td>
                <td>{item.last_period ?? "—"}</td>
                <td><Growth value={item.last_sales_yoy_pct} /></td>
                <td><Growth value={item.last_profit_yoy_pct} /></td>
                <td>{formatRatio(item.pe)}</td>
              </tr>
            ))}
          </tbody>
        </table>
      </div>
    </section>
  );
}

export function ResultsPanel({ watchlistSymbols, onOpenChart }: ResultsPanelProps) {
  const saved = useMemo(readSaved, []);
  const [view, setView] = useState<ResultsView>(saved.view);
  const [filters, setFilters] = useState<ResultsFilters>(saved.filters);
  const [basis, setBasis] = useState<ResultsBasis>(saved.basis);
  const today = todayIst();
  const [range, setRange] = useState(() => ({ from: seasonStart(today), to: today }));
  const [quarterly, setQuarterly] = useState<QuarterlyResultsResponse | null>(null);
  const [upcoming, setUpcoming] = useState<UpcomingResultsResponse | null>(null);
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState<string | null>(null);
  const [shown, setShown] = useState(PAGE_SIZE);
  const [allSectors, setAllSectors] = useState(false);
  const [filtersOpen, setFiltersOpen] = useState(false);

  useEffect(() => {
    try {
      window.localStorage.setItem(STORAGE_KEY, JSON.stringify({ view, basis, filters: { ...filters, search: "" } }));
    } catch {
      /* storage blocked: the page still works, it just forgets */
    }
  }, [view, basis, filters]);

  const load = useCallback(
    async (quiet = false) => {
      if (!quiet) setLoading(true);
      try {
        if (view === "quarterly") setQuarterly(await getQuarterlyResults(range.from, range.to));
        else setUpcoming(await getUpcomingResults(45));
        setError(null);
      } catch (err) {
        setError(err instanceof Error ? err.message : "Could not load results");
      } finally {
        if (!quiet) setLoading(false);
      }
    },
    [view, range.from, range.to],
  );

  useEffect(() => {
    void load();
  }, [load]);

  // While today's results are on screen, re-read every few minutes: the
  // evening runs land from 7 PM.
  useEffect(() => {
    if (view !== "quarterly" || range.to < today) return;
    const timer = window.setInterval(() => {
      if (document.visibilityState === "visible") void load(true);
    }, REFRESH_MS);
    const onVisible = () => {
      if (document.visibilityState === "visible") void load(true);
    };
    document.addEventListener("visibilitychange", onVisible);
    return () => {
      window.clearInterval(timer);
      document.removeEventListener("visibilitychange", onVisible);
    };
  }, [view, range.to, today, load]);

  useEffect(() => setShown(PAGE_SIZE), [filters, range, view]);

  const watchlist = useMemo(() => new Set(watchlistSymbols.map((s) => s.toUpperCase())), [watchlistSymbols]);
  const sourceItems: { sector: string | null }[] = view === "quarterly" ? quarterly?.items ?? [] : upcoming?.items ?? [];
  const sectors = useMemo(() => sectorCounts(sourceItems), [sourceItems]);

  const quarterlyShown = useMemo(() => {
    const shownItems = (quarterly?.items ?? []).map((item) => showBasis(item, basis)).filter((item): item is ShownResult => item !== null);
    return sortResults(filterResults(shownItems, filters, watchlist), filters.sort);
  }, [quarterly, basis, filters, watchlist]);
  const upcomingShown = useMemo(
    () => filterResults((upcoming?.items ?? []).map((item) => showUpcomingBasis(item, basis)), filters, watchlist),
    [upcoming, basis, filters, watchlist],
  );
  const upcomingDays = useMemo(() => {
    const days: { date: string; items: ShownUpcoming[] }[] = [];
    for (const item of upcomingShown) {
      const last = days[days.length - 1];
      if (last && last.date === item.date) last.items.push(item);
      else days.push({ date: item.date, items: [item] });
    }
    return days;
  }, [upcomingShown]);

  const visibleSymbols = useMemo(
    () => (view === "quarterly" ? quarterlyShown : upcomingShown).map((item) => item.symbol),
    [view, quarterlyShown, upcomingShown],
  );
  const openChart = (symbol: string) => onOpenChart(symbol, visibleSymbols);

  const presets = rangePresets(today);
  const activePreset = presets.find((p) => p.from === range.from && p.to === range.to)?.key ?? null;
  const filterCount = filters.caps.length + filters.sectors.length + (filters.scope === "watchlist" ? 1 : 0);
  const pageItems = quarterlyShown.slice(0, shown);
  const groups = filters.sort === "latest" ? groupByDay(pageItems) : [{ date: "", items: pageItems }];
  const checked = quarterly?.feed_checked_at ? new Date(quarterly.feed_checked_at) : null;
  const dayCount = (date: string) => {
    const count = quarterlyShown.filter((item) => item.declared_date === date).length;
    return `${count.toLocaleString("en-IN")} ${count === 1 ? "result" : "results"}`;
  };

  return (
    <section className="results-page" aria-label="Results">
      <header className="results-top">
        <div>
          <h2 className="results-title">{view === "quarterly" ? "Latest Quarterly Results" : "Upcoming Results"}</h2>
          <p className="results-sub">
            Consolidated figures from NSE's XBRL filings, standalone from BSE, in ₹ crore. Updated daily at 7:00 PM IST with that day's results; late filers follow at 9:30 PM and 11:45 PM.
          </p>
        </div>
        <div className="results-tabs" role="tablist" aria-label="Results view">
          <button type="button" role="tab" aria-selected={view === "quarterly"} className={view === "quarterly" ? "is-active" : ""} onClick={() => setView("quarterly")}>
            Quarterly Results
          </button>
          <button type="button" role="tab" aria-selected={view === "upcoming"} className={view === "upcoming" ? "is-active" : ""} onClick={() => setView("upcoming")}>
            Upcoming
          </button>
        </div>
      </header>

      <div className="results-toolbar">
        {view === "quarterly" ? (
          <div className="results-range">
            <CalendarDays size={15} aria-hidden />
            <input type="date" aria-label="From" value={range.from} max={range.to} onChange={(e) => e.target.value && setRange((r) => ({ ...r, from: e.target.value }))} />
            <span aria-hidden>–</span>
            <input type="date" aria-label="To" value={range.to} min={range.from} max={today} onChange={(e) => e.target.value && setRange((r) => ({ ...r, to: e.target.value }))} />
            <div className="results-presets">
              {presets.map((preset) => (
                <button
                  key={preset.key}
                  type="button"
                  className={activePreset === preset.key ? "is-active" : ""}
                  onClick={() => setRange({ from: preset.from, to: preset.to })}
                >
                  {preset.label}
                </button>
              ))}
            </div>
          </div>
        ) : (
          <p className="results-range-note">Board meetings for results in the next 45 days, from the BSE and NSE calendars.</p>
        )}
        <div className="results-basis-toggle" role="radiogroup" aria-label="Figures">
          {(["consolidated", "standalone"] as const).map((key) => (
            <button key={key} type="button" role="radio" aria-checked={basis === key} className={basis === key ? "is-active" : ""} onClick={() => setBasis(key)}>
              {BASIS_LABEL[key]}
            </button>
          ))}
        </div>
        <label className="results-search">
          <Search size={15} aria-hidden />
          <input
            type="search"
            placeholder="Search company"
            value={filters.search}
            onChange={(e) => setFilters((f) => ({ ...f, search: e.target.value }))}
          />
        </label>
        <button type="button" className="results-refresh" onClick={() => void load()} disabled={loading} title="Reload">
          <RefreshCw size={14} className={loading ? "is-spinning" : ""} aria-hidden />
        </button>
        <button type="button" className="results-filters-toggle" onClick={() => setFiltersOpen((open) => !open)} aria-expanded={filtersOpen}>
          Filters{filterCount ? ` (${filterCount})` : ""}
        </button>
      </div>

      <div className="results-layout">
        <aside className={`results-filters${filtersOpen ? " is-open" : ""}`} aria-label="Filters">
          <fieldset>
            <legend>By market cap</legend>
            {CAP_OPTIONS.map((option) => (
              <label key={option.key} title={option.hint}>
                <input
                  type="checkbox"
                  checked={filters.caps.includes(option.key)}
                  onChange={() => setFilters((f) => ({ ...f, caps: toggle(f.caps, option.key) }))}
                />
                {option.label}
              </label>
            ))}
          </fieldset>
          {view === "quarterly" ? (
            <fieldset>
              <legend>By sorting</legend>
              {SORT_OPTIONS.map((option) => (
                <label key={option.key}>
                  <input type="radio" name="results-sort" checked={filters.sort === option.key} onChange={() => setFilters((f) => ({ ...f, sort: option.key }))} />
                  {option.label}
                </label>
              ))}
            </fieldset>
          ) : null}
          <fieldset>
            <legend>By companies</legend>
            <label title={watchlist.size ? undefined : "Your watchlists are empty"}>
              <input type="radio" name="results-scope" checked={filters.scope === "watchlist"} onChange={() => setFilters((f) => ({ ...f, scope: "watchlist" }))} />
              My watchlists
            </label>
            <label>
              <input type="radio" name="results-scope" checked={filters.scope === "all"} onChange={() => setFilters((f) => ({ ...f, scope: "all" }))} />
              All companies
            </label>
          </fieldset>
          <fieldset>
            <legend>By sectors</legend>
            {(allSectors ? sectors : sectors.slice(0, SECTORS_SHOWN)).map(([sector, count]) => (
              <label key={sector}>
                <input
                  type="checkbox"
                  checked={filters.sectors.includes(sector)}
                  onChange={() => setFilters((f) => ({ ...f, sectors: toggle(f.sectors, sector) }))}
                />
                <span className="results-filter-text">{sector}</span>
                <span className="results-filter-count">{count}</span>
              </label>
            ))}
            {sectors.length > SECTORS_SHOWN ? (
              <button type="button" className="results-more-link" onClick={() => setAllSectors((all) => !all)}>
                {allSectors ? "Show fewer" : `+ View ${sectors.length - SECTORS_SHOWN} more`}
              </button>
            ) : null}
            {!sectors.length ? <p className="results-muted">No sectors in this range.</p> : null}
          </fieldset>
          {filterCount ? (
            <button type="button" className="results-clear" onClick={() => setFilters((f) => ({ ...DEFAULT_FILTERS, sort: f.sort, search: f.search }))}>
              Clear filters
            </button>
          ) : null}
        </aside>

        <div className="results-main">
          {error ? <p className="results-error">{error}</p> : null}
          {view === "quarterly" ? (
            <>
              <p className="results-count">
                {loading && !quarterly
                  ? "Loading results…"
                  : `${quarterlyShown.length.toLocaleString("en-IN")} ${quarterlyShown.length === 1 ? "result" : "results"} · ${formatDay(range.from)} – ${formatDay(range.to)}`}
                {checked && !Number.isNaN(checked.getTime()) ? (
                  <span className="results-muted"> · BSE filings last read {checked.toLocaleString("en-IN", { day: "numeric", month: "short", hour: "numeric", minute: "2-digit", timeZone: "Asia/Kolkata" })} IST</span>
                ) : null}
              </p>
              {quarterly?.awaiting_figures?.length ? (
                <p className="results-awaiting">
                  Filed, figures not yet on BSE ({quarterly.awaiting_figures.length}):{" "}
                  {quarterly.awaiting_figures.slice(0, 12).map((c) => c.name).join(", ")}
                  {quarterly.awaiting_figures.length > 12 ? "…" : ""}. They appear at the next update.
                </p>
              ) : null}
              {!loading && quarterly && !quarterlyShown.length ? (
                <p className="results-empty">
                  {quarterly.items.length
                    ? "No results match these filters."
                    : range.to >= today && range.from >= seasonStart(today)
                      ? "No results declared in this range yet. The day's results arrive at 7:00 PM IST."
                      : "No results declared in this range."}
                </p>
              ) : null}
              {groups.map((group) => (
                <section key={group.date || "all"} className="results-day">
                  {group.date ? (
                    <h3 className="results-day-head">
                      {formatDay(group.date, true)}{" "}
                      <span>{dayCount(group.date)}</span>
                    </h3>
                  ) : null}
                  {group.items.map((item) => (
                    <ResultCard key={`${item.symbol}-${item.period}`} item={item} basis={basis} onOpen={openChart} />
                  ))}
                </section>
              ))}
              {quarterlyShown.length > shown ? (
                <button type="button" className="results-show-more" onClick={() => setShown((n) => n + PAGE_SIZE)}>
                  Show {Math.min(PAGE_SIZE, quarterlyShown.length - shown)} more of {(quarterlyShown.length - shown).toLocaleString("en-IN")}
                </button>
              ) : null}
            </>
          ) : (
            <>
              <p className="results-count">
                {loading && !upcoming ? "Loading calendar…" : `${upcomingShown.length.toLocaleString("en-IN")} companies scheduled`}
              </p>
              {!loading && upcoming && !upcomingShown.length ? (
                <p className="results-empty">{upcoming.items.length ? "No companies match these filters." : "No results board meetings announced yet."}</p>
              ) : null}
              {upcomingDays.map((day) => (
                <UpcomingDay key={day.date} date={day.date} items={day.items} basis={basis} onOpen={openChart} />
              ))}
            </>
          )}
        </div>
      </div>
    </section>
  );
}
