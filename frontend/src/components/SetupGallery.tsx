import { useEffect, useMemo, useState } from "react";
import { ExternalLink } from "lucide-react";

import {
  getGalleryIndianHistory,
  getGalleryTraderCharts,
  getLookalikeGallery,
  type GalleryHistoryRow,
  type GalleryIndex,
  type GalleryOutcome,
  type LookalikeMatch,
  type LookalikeRefRow,
  type LookalikeStyleNotes,
  type Lookalikes,
} from "../lib/api";
import { fullChartUrl } from "../lib/chartLink";
import { ChartsPerRow, useChartHeight, useChartsPerRow } from "./ChartsPerRow";
import { LookalikeChart, type LookalikeSeries } from "./LookalikeChart";

/* The setup gallery: every chart of one kind in one place, to train the eye.

   Trader → setup → two branches:
     • the trader's own charts of that setup (his newsletter / ideas, redrawn
       from prices at their date, with what followed);
     • Indian charts — "History" (charts that looked like the setup on past
       Fridays, with what followed) and "Today" (today's strongest matches).
   "Hide what happened next" turns any page into a quiz: each card shows the
   chart up to its date and reveals the rest on a click. */

const SETUP_ORDER = ["", "flag", "cup_handle", "base", "channel", "triangle", "double_bottom", "gap", "trendline", "wedge", "head_shoulders"];
const SETUP_LABEL: Record<string, string> = {
  "": "All his charts",
  flag: "Flag & pennant",
  cup_handle: "Cup & handle",
  base: "Base",
  channel: "Channel",
  triangle: "Triangle",
  double_bottom: "Double bottom",
  gap: "Gap",
  trendline: "Trendline break",
  wedge: "Wedge",
  head_shoulders: "Head & shoulders",
};
const TRADER_LABEL: Record<string, string> = { zanger: "Dan Zanger", minervini: "Mark Minervini" };
const PAGE = 24;
const COLS_KEY = "mr-malik-setup-gallery-cols:v2";
const TODAY_MIN_PCT = 95;

type Branch = "trader" | "india";
type IndiaView = "history" | "today";

function fmtDate(iso?: string | null) {
  if (!iso) return "—";
  const d = new Date(`${iso}T00:00:00`);
  return Number.isNaN(d.getTime()) ? iso : d.toLocaleDateString("en-IN", { day: "numeric", month: "short", year: "numeric" });
}

function Outcome({ label, gain, loss, days }: { label?: string; gain?: number | null; loss?: number | null; days?: number | null }) {
  if (label === "worked")
    return (
      <span className="lookalike-chip is-worked">
        Rose 20%{days ? ` in ${days} sessions` : ""}
        {gain != null ? ` · best +${gain.toFixed(0)}%` : ""}
      </span>
    );
  if (label === "failed")
    return (
      <span className="lookalike-chip is-failed">
        Failed{loss != null ? ` · fell ${Math.abs(loss).toFixed(0)}%` : ""}
      </span>
    );
  return <span className="lookalike-chip is-pending">Still running</span>;
}

/** In practice mode a card shows the chart only up to its date; one click
    reveals what followed and the result together. */
function useReveal(hideAfter: boolean) {
  const [revealed, setRevealed] = useState(false);
  useEffect(() => setRevealed(false), [hideAfter]);
  return { hidden: hideAfter && !revealed, reveal: () => setRevealed(true) };
}

function ChartBox({
  data,
  height,
  hidden,
  onReveal,
  label,
}: {
  data: LookalikeSeries | null | undefined;
  height: number;
  hidden: boolean;
  onReveal: () => void;
  label: string;
}) {
  return (
    <div className="setup-gallery-chart">
      <LookalikeChart data={data} height={height} labels showAfter={!hidden} ariaLabel={label} />
      {hidden && (data?.c?.length ?? 0) > (data?.setup_index ?? 0) + 1 ? (
        <button type="button" className="setup-gallery-reveal" onClick={onReveal}>
          Show what happened next
        </button>
      ) : null}
    </div>
  );
}

function TraderCard({ row, height, hideAfter }: { row: LookalikeRefRow; height: number; hideAfter: boolean }) {
  const { hidden, reveal } = useReveal(hideAfter);
  return (
    <article className="setup-gallery-card">
      <ChartBox data={row.chart} height={height} hidden={hidden} onReveal={reveal} label={`${row.ticker} on ${row.date}`} />
      <div className="setup-gallery-meta">
        <a href={row.link} target="_blank" rel="noreferrer noopener" title="Open on TradingView">
          <strong>{row.ticker}</strong> · {fmtDate(row.date)} <ExternalLink size={11} />
        </a>
        {hidden ? null : <Outcome label={row.label} gain={row.max_gain_pct} loss={row.max_loss_pct} days={row.days_to_result} />}
        {row.prices === "chart" ? <span className="lookalike-tag">prices read from his chart</span> : null}
      </div>
    </article>
  );
}

function IndianHistoryCard({ row, height, hideAfter }: { row: GalleryHistoryRow; height: number; hideAfter: boolean }) {
  const { hidden, reveal } = useReveal(hideAfter);
  return (
    <article className="setup-gallery-card">
      {row.chart ? (
        <ChartBox
          data={row.chart as unknown as LookalikeSeries}
          height={height}
          hidden={hidden}
          onReveal={reveal}
          label={`${row.symbol} on ${row.date}`}
        />
      ) : (
        <div className="lookalike-canvas lookalike-missing" style={{ height }}>
          Chart unavailable
        </div>
      )}
      <div className="setup-gallery-meta">
        <a href={fullChartUrl(row.symbol)} target="_blank" rel="noreferrer noopener" title="Open on my site in a new tab">
          <strong>{row.symbol}</strong> · {fmtDate(row.session ?? row.date)} <ExternalLink size={11} />
        </a>
        <span className="lookalike-stat-sub">beat {row.pct.toFixed(0)}% of ordinary charts</span>
        {hidden ? null : <Outcome label={row.label} gain={row.gain} loss={row.loss} days={row.days} />}
      </div>
    </article>
  );
}

function TodayCard({ match, height }: { match: LookalikeMatch; height: number }) {
  return (
    <article className="setup-gallery-card">
      <a className="lookalike-chart-button" href={fullChartUrl(match.symbol)} target="_blank" rel="noreferrer noopener" title="Open on my site in a new tab">
        <LookalikeChart data={match.window as unknown as LookalikeSeries} height={height} labels ariaLabel={`${match.symbol} today`} />
      </a>
      <div className="setup-gallery-meta">
        <a href={fullChartUrl(match.symbol)} target="_blank" rel="noreferrer noopener">
          <strong>{match.symbol}</strong> · ₹{match.close.toLocaleString("en-IN")} <ExternalLink size={11} />
        </a>
        <span className="lookalike-stat-sub">
          beats {match.percentile.toFixed(0)}% of ordinary charts · ₹{match.turnover_crore.toFixed(0)} cr/day
        </span>
      </div>
      {match.reason ? (
        <details className="lookalike-why">
          <summary>Why</summary>
          <p className="lookalike-reason">{match.reason}</p>
        </details>
      ) : null}
    </article>
  );
}

const NOTE_SECTIONS: Array<[keyof LookalikeStyleNotes, string]> = [
  ["what_it_looks_like", "What it looks like"],
  ["buy_point", "Where he buys"],
  ["volume", "Volume"],
  ["stops_and_exits", "Stops and selling"],
  ["what_makes_it_fail", "What makes it fail"],
  ["what_he_buys", "What he buys"],
  ["entry_rules", "Entry"],
  ["risk_rules", "Risk"],
  ["selling_rules", "Selling"],
  ["market_timing", "The market"],
];

/* How the trader describes this setup — our summary of his comments, never his words. */
export function StyleNotes({ notes, open = true }: { notes?: LookalikeStyleNotes | null; open?: boolean }) {
  if (!notes?.summary) return null;
  const vocab = Object.entries(notes.his_vocabulary ?? {});
  return (
    <details className="lookalike-curve-wrap lookalike-notes" open={open}>
      <summary>
        <h3>How he describes it</h3>
        <span className="lookalike-stat-sub">
          Our summary of {notes.evidence?.comments_read?.toLocaleString("en-IN") ?? "his"} chart comments
          {notes.evidence?.years ? `, ${notes.evidence.years}` : ""} — in our words, not his
        </span>
      </summary>
      <p>{notes.summary}</p>
      <div className="lookalike-notes-grid">
        {NOTE_SECTIONS.map(([key, label]) => {
          const items = notes[key];
          return Array.isArray(items) && items.length ? (
            <div key={key}>
              <h4>{label}</h4>
              <ul>
                {items.map((t, i) => (
                  <li key={i}>{t}</li>
                ))}
              </ul>
            </div>
          ) : null;
        })}
        {vocab.length ? (
          <div>
            <h4>His terms</h4>
            <ul>
              {vocab.map(([term, meaning]) => (
                <li key={term}>
                  <strong>{term}</strong> — {meaning}
                </li>
              ))}
            </ul>
          </div>
        ) : null}
      </div>
      {notes.changes_over_time ? <p className="lookalike-stat-sub">{notes.changes_over_time}</p> : null}
    </details>
  );
}

function usePaged<T>(load: (page: number) => Promise<{ total: number; rows: T[] }>, deps: unknown[]) {
  const [rows, setRows] = useState<T[]>([]);
  const [total, setTotal] = useState(0);
  const [page, setPage] = useState(0);
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState<string | null>(null);
  useEffect(() => {
    setRows([]);
    setTotal(0);
    setPage(0);
  }, deps); // eslint-disable-line react-hooks/exhaustive-deps
  useEffect(() => {
    let cancelled = false;
    setLoading(true);
    setError(null);
    load(page)
      .then((res) => {
        if (cancelled) return;
        setTotal(res.total);
        setRows((prev) => (page === 0 ? res.rows : [...prev, ...res.rows]));
      })
      .catch((e) => !cancelled && setError(e instanceof Error ? e.message : "Could not load the charts."))
      .finally(() => !cancelled && setLoading(false));
    return () => {
      cancelled = true;
    };
  }, [page, ...deps]); // eslint-disable-line react-hooks/exhaustive-deps
  return { rows, total, loading, error, more: () => setPage((p) => p + 1) };
}

function TraderGrid({ style, outcome, cols, height, hideAfter }: { style: string; outcome: GalleryOutcome; cols: number; height: number; hideAfter: boolean }) {
  const { rows, total, loading, error, more } = usePaged((page) => getGalleryTraderCharts(style, page, PAGE, outcome), [style, outcome]);
  return (
    <Grid count={rows.length} total={total} loading={loading} error={error} more={more} cols={cols}>
      {rows.map((r) => (
        <TraderCard key={`${r.ticker}@${r.date}`} row={r} height={height} hideAfter={hideAfter} />
      ))}
    </Grid>
  );
}

function IndiaHistoryGrid({ style, outcome, cols, height, hideAfter }: { style: string; outcome: GalleryOutcome; cols: number; height: number; hideAfter: boolean }) {
  const { rows, total, loading, error, more } = usePaged((page) => getGalleryIndianHistory(style, page, PAGE, outcome), [style, outcome]);
  return (
    <Grid count={rows.length} total={total} loading={loading} error={error} more={more} cols={cols}>
      {rows.map((r) => (
        <IndianHistoryCard key={`${r.symbol}@${r.date}`} row={r} height={height} hideAfter={hideAfter} />
      ))}
    </Grid>
  );
}

function Grid({
  count,
  total,
  loading,
  error,
  more,
  cols,
  children,
}: {
  count: number;
  total: number;
  loading: boolean;
  error: string | null;
  more: () => void;
  cols: number;
  children: React.ReactNode;
}) {
  return (
    <>
      <p className="lookalike-stat-sub">
        {loading && !count ? "Loading…" : `Showing ${count.toLocaleString("en-IN")} of ${total.toLocaleString("en-IN")} charts`}
      </p>
      {error ? <p className="lookalike-empty">{error}</p> : null}
      <div className="setup-gallery-grid" style={{ gridTemplateColumns: `repeat(${cols}, minmax(0, 1fr))` }}>
        {children}
      </div>
      {count < total ? (
        <button type="button" className="lookalike-filter setup-gallery-more" onClick={more} disabled={loading}>
          {loading ? "Loading…" : `Show ${Math.min(PAGE, total - count)} more`}
        </button>
      ) : null}
    </>
  );
}

export function SetupGallery({ today }: { today: Lookalikes | null }) {
  const [index, setIndex] = useState<GalleryIndex | null>(null);
  const [error, setError] = useState<string | null>(null);
  const [trader, setTrader] = useState("zanger");
  const [setup, setSetup] = useState("flag");
  const [branch, setBranch] = useState<Branch>("trader");
  const [indiaView, setIndiaView] = useState<IndiaView>("history");
  const [outcome, setOutcome] = useState<GalleryOutcome>("all");
  const [hideAfter, setHideAfter] = useState(false);
  const [cols, setCols] = useChartsPerRow(COLS_KEY, 3);
  const height = useChartHeight(cols);
  useEffect(() => {
    getLookalikeGallery()
      .then(setIndex)
      .catch((e) => setError(e instanceof Error ? e.message : "Could not load the setup gallery."));
  }, []);

  const traders = useMemo(() => {
    const out: Record<string, string[]> = {};
    for (const style of Object.keys(index?.styles ?? {})) {
      const [t, ...rest] = style.split("_");
      (out[t] ??= []).push(rest.join("_"));
    }
    for (const t of Object.keys(out)) out[t].sort((a, b) => SETUP_ORDER.indexOf(a) - SETUP_ORDER.indexOf(b));
    return out;
  }, [index]);

  const setups = traders[trader] ?? [];
  const activeSetup = setups.includes(setup) ? setup : setups[0] ?? "";
  const style = activeSetup ? `${trader}_${activeSetup}` : trader;
  const info = index?.styles?.[style];
  const todayMatches = useMemo(() => {
    if (!today || !today.available) return [];
    return (today.styles?.[style]?.matches ?? []).filter((m) => m.percentile >= TODAY_MIN_PCT);
  }, [today, style]);

  if (error) return <p className="lookalike-empty">{error}</p>;
  if (!index) return <p className="lookalike-empty">Loading the setup gallery…</p>;
  if (!index.available) return <p className="lookalike-empty">The setup gallery is built with the next evening run.</p>;

  const traderName = TRADER_LABEL[trader] ?? trader;
  const setupName = SETUP_LABEL[activeSetup] ?? activeSetup.replace(/_/g, " ");
  // "flag & pennant charts", or plain "charts" for a trader's whole style
  const kind = activeSetup ? `${setupName.toLowerCase()} charts` : "charts";
  const rule = index.history_rule;

  return (
    <div className="setup-gallery">
      <div className="lookalike-styles" role="tablist" aria-label="Whose setups">
        {Object.keys(traders).map((t) => (
          <button
            key={t}
            type="button"
            role="tab"
            aria-selected={t === trader}
            className={`bot-view-tab${t === trader ? " is-active" : ""}`}
            onClick={() => setTrader(t)}
          >
            {TRADER_LABEL[t] ?? t}
          </button>
        ))}
      </div>

      <div className="lookalike-styles" role="tablist" aria-label="Setup">
        {setups.map((s) => {
          const key = s ? `${trader}_${s}` : trader;
          const n = index.styles[key]?.trader_charts ?? 0;
          return (
            <button
              key={key}
              type="button"
              role="tab"
              aria-selected={s === activeSetup}
              className={`lookalike-filter${s === activeSetup ? " is-active" : ""}`}
              onClick={() => setSetup(s)}
            >
              {trader === "minervini" && !s ? "His setups" : SETUP_LABEL[s] ?? s} · {n.toLocaleString("en-IN")}
            </button>
          );
        })}
      </div>

      <div className="lookalike-views" role="tablist" aria-label="Whose charts">
        <button
          type="button"
          role="tab"
          aria-selected={branch === "trader"}
          className={`bot-view-tab${branch === "trader" ? " is-active" : ""}`}
          onClick={() => setBranch("trader")}
        >
          {traderName}'s charts · {(info?.trader_charts ?? 0).toLocaleString("en-IN")}
        </button>
        <button
          type="button"
          role="tab"
          aria-selected={branch === "india"}
          className={`bot-view-tab${branch === "india" ? " is-active" : ""}`}
          onClick={() => setBranch("india")}
        >
          Indian charts
        </button>
      </div>

      {branch === "india" ? (
        <div className="lookalike-views" role="tablist" aria-label="When">
          <button
            type="button"
            role="tab"
            aria-selected={indiaView === "history"}
            className={`lookalike-filter${indiaView === "history" ? " is-active" : ""}`}
            onClick={() => setIndiaView("history")}
          >
            History · {(info?.india_history ?? 0).toLocaleString("en-IN")}
          </button>
          <button
            type="button"
            role="tab"
            aria-selected={indiaView === "today"}
            className={`lookalike-filter${indiaView === "today" ? " is-active" : ""}`}
            onClick={() => setIndiaView("today")}
          >
            Tradable today · {todayMatches.length}
          </button>
        </div>
      ) : null}

      <div className="setup-gallery-controls">
        {!(branch === "india" && indiaView === "today") ? (
          <label>
            Result{" "}
            <select value={outcome} onChange={(e) => setOutcome(e.target.value as GalleryOutcome)}>
              <option value="all">All</option>
              <option value="worked">Worked</option>
              <option value="failed">Failed</option>
              <option value="pending">Still running</option>
            </select>
          </label>
        ) : null}
        {!(branch === "india" && indiaView === "today") ? (
          <label className="setup-gallery-toggle">
            <input type="checkbox" checked={hideAfter} onChange={(e) => setHideAfter(e.target.checked)} /> Practice: hide what happened next
          </label>
        ) : null}
        <ChartsPerRow value={cols} onChange={setCols} />
      </div>

      <StyleNotes notes={info?.notes} open={false} />

      {branch === "trader" ? (
        <>
          <p className="lookalike-stat-sub">
            Every {activeSetup ? `${setupName.toLowerCase()} chart` : "chart"} {traderName} showed, redrawn from prices at its own date; the shaded part is
            what followed. Worked = rose {rule?.target_pct ?? 20}% before falling {rule?.stop_pct ?? 8}% within{" "}
            {rule?.horizon_sessions ?? 40} sessions.
            {info?.worked_rate_pct?.setups != null && info.worked_rate_pct.ordinary_days != null
              ? ` His ${kind} worked ${info.worked_rate_pct.setups.toFixed(0)}% of the time, against ${info.worked_rate_pct.ordinary_days.toFixed(0)}% for ordinary days in the same stocks.`
              : ""}
          </p>
          <TraderGrid style={style} outcome={outcome} cols={cols} height={height} hideAfter={hideAfter} />
        </>
      ) : !info?.shown ? (
        <p className="lookalike-empty">
          The model cannot yet tell {traderName}'s {kind} from ordinary charts reliably (
          {info?.recognition != null ? `${(info.recognition * 100).toFixed(0)}%` : "—"}, where 50% is a coin flip), so no Indian
          charts are matched to it. His own charts are still worth studying.
        </p>
      ) : indiaView === "history" ? (
        <>
          <p className="lookalike-stat-sub">
            Each Friday since {rule?.from ? rule.from.slice(0, 4) : "2001"}, the {rule?.top_per_day ?? 3} Indian charts that looked most like {traderName}'s {kind} (beating {rule?.min_percentile ?? 97}% of ordinary charts), each stock at most once in 8
            weeks. These are look-alikes for study, not past picks — the library includes his later charts, and only companies
            listed today are included (few before 2003).
          </p>
          <IndiaHistoryGrid style={style} outcome={outcome} cols={cols} height={height} hideAfter={hideAfter} />
        </>
      ) : (
        <>
          <p className="lookalike-stat-sub">
            Today's Indian charts that look like {traderName}'s {kind} more than {TODAY_MIN_PCT}% of ordinary
            charts do, with at least ₹2 cr a day of turnover. A resemblance, not a buy signal.
          </p>
          {todayMatches.length ? (
            <div className="setup-gallery-grid" style={{ gridTemplateColumns: `repeat(${cols}, minmax(0, 1fr))` }}>
              {todayMatches.map((m) => (
                <TodayCard key={m.symbol} match={m} height={height} />
              ))}
            </div>
          ) : (
            <p className="lookalike-empty">No Indian chart looks enough like this setup today.</p>
          )}
        </>
      )}
    </div>
  );
}
