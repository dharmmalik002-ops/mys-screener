import { useCallback, useEffect, useRef, useState } from "react";
import { AlertTriangle, Search } from "lucide-react";
import {
  getSignalArchive,
  type ArchiveQuery,
  type ArchiveStats,
  type SignalArchive,
} from "../lib/api";
import "./SignalArchivePanel.css";

type Props = { onOpenSymbolChart?: (symbol: string) => void };

const SETUPS = [
  { value: null, label: "Both setups" },
  { value: "vcp", label: "VCP" },
  { value: "high-tight-flag", label: "High tight flag" },
];

const RESULTS = [
  { value: null, label: "Every outcome" },
  { value: "loss", label: "Failures only" },
  { value: "win", label: "Winners only" },
  { value: "timeout", label: "Went nowhere" },
];

const SORTS = [
  { value: "recent", label: "Most recent" },
  { value: "worst", label: "Worst outcome" },
  { value: "best", label: "Best outcome" },
  { value: "biggest_run", label: "Biggest run-up" },
  { value: "rs", label: "Highest RS" },
];

function pct(value: number | null | undefined, digits = 1): string {
  if (value === null || value === undefined) return "—";
  return `${value > 0 ? "+" : ""}${value.toFixed(digits)}%`;
}

/** The comparison is the lesson: a win rate beside its baseline says something
 *  a win rate alone never can. */
function Delta({ stats, baseline }: { stats: ArchiveStats; baseline: ArchiveStats }) {
  if (stats.win_rate === null || baseline.win_rate === null || stats.count === 0) return null;
  const delta = stats.win_rate - baseline.win_rate;
  if (Math.abs(delta) < 0.05) return <span className="sar-delta sar-delta--flat">same as baseline</span>;
  return (
    <span className={delta > 0 ? "sar-delta sar-delta--up" : "sar-delta sar-delta--down"}>
      {delta > 0 ? "+" : ""}{delta.toFixed(1)} pts vs baseline
    </span>
  );
}

/**
 * The archive of what actually happened — failures included, first.
 *
 * Pattern teaching almost always shows the winners, which trains the eye to
 * see a breakout in every base. This defaults to nothing filtered and lets the
 * first click be "failures only", because the losses are where the information
 * is: what did the 400 VCPs that did not work have in common?
 *
 * Every query reports the statistics for the whole match beside the same
 * statistics for the unfiltered corpus. Paging never changes them — a win rate
 * that moved as you scrolled would be worse than none at all.
 */
export function SignalArchivePanel({ onOpenSymbolChart }: Props) {
  const [data, setData] = useState<SignalArchive | null>(null);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState<string | null>(null);
  const [query, setQuery] = useState<ArchiveQuery>({ sort: "recent", limit: 50, offset: 0 });
  const [symbolInput, setSymbolInput] = useState("");
  const requestId = useRef(0);

  const load = useCallback(async (next: ArchiveQuery) => {
    const id = ++requestId.current;
    setLoading(true);
    try {
      const result = await getSignalArchive(next);
      if (id !== requestId.current) return;
      setData(result);
      setError(null);
    } catch (err) {
      if (id !== requestId.current) return;
      setError(err instanceof Error ? err.message : "Could not load the archive.");
    } finally {
      if (id === requestId.current) setLoading(false);
    }
  }, []);

  useEffect(() => {
    void load(query);
  }, [load, query]);

  // Any filter change resets paging: keeping the offset would land the user on
  // page 4 of a result set that now has two pages.
  const patch = (next: Partial<ArchiveQuery>) => setQuery((prev) => ({ ...prev, ...next, offset: 0 }));

  const stats = data?.stats;
  const baseline = data?.baseline;
  const page = data ? Math.floor(data.offset / Math.max(1, data.limit)) + 1 : 1;
  const pages = data ? Math.max(1, Math.ceil(data.total / Math.max(1, data.limit))) : 1;

  return (
    <section className="sar">
      <header className="sar-head">
        <div>
          <p className="sar-eyebrow">Signal archive</p>
          <h3>What actually happened, failures included</h3>
        </div>
        {data?.available ? (
          <span className="sar-count">{data.total.toLocaleString("en-IN")} signals</span>
        ) : null}
      </header>

      <div className="sar-filters">
        {SETUPS.map((option) => (
          <button
            key={option.label}
            type="button"
            className={query.setup === option.value ? "sar-chip sar-chip--on" : "sar-chip"}
            onClick={() => patch({ setup: option.value })}
          >
            {option.label}
          </button>
        ))}
        <span className="sar-sep" aria-hidden />
        {RESULTS.map((option) => (
          <button
            key={option.label}
            type="button"
            className={query.result === option.value ? "sar-chip sar-chip--on" : "sar-chip"}
            onClick={() => patch({ result: option.value })}
          >
            {option.label}
          </button>
        ))}
      </div>

      <div className="sar-filters sar-filters--fine">
        <label className="sar-field">
          <span>RS at least</span>
          <input
            type="number" min={0} max={99} placeholder="any"
            value={query.rsMin ?? ""}
            onChange={(event) => patch({ rsMin: event.target.value === "" ? null : Number(event.target.value) })}
          />
        </label>
        <label className="sar-field">
          <span>Risk at most %</span>
          <input
            type="number" step="0.5" min={0.5} placeholder="any"
            value={query.riskMax ?? ""}
            onChange={(event) => patch({ riskMax: event.target.value === "" ? null : Number(event.target.value) })}
          />
        </label>
        <button
          type="button"
          className={query.groupTopDecile ? "sar-chip sar-chip--on" : "sar-chip"}
          onClick={() => patch({ groupTopDecile: query.groupTopDecile ? null : true })}
        >
          Top-decile group only
        </button>
        <form
          className="sar-search"
          onSubmit={(event) => {
            event.preventDefault();
            patch({ symbol: symbolInput.trim().toUpperCase() || null });
          }}
        >
          <Search size={13} aria-hidden />
          <input
            value={symbolInput}
            onChange={(event) => setSymbolInput(event.target.value)}
            placeholder="symbol"
            aria-label="Filter by symbol"
          />
        </form>
        <label className="sar-field">
          <span>Order by</span>
          <select value={query.sort} onChange={(event) => patch({ sort: event.target.value })}>
            {SORTS.map((option) => (
              <option key={option.value} value={option.value}>{option.label}</option>
            ))}
          </select>
        </label>
      </div>

      {error ? (
        <p className="sar-empty"><AlertTriangle size={14} aria-hidden /> {error}</p>
      ) : null}

      {data && !data.available ? (
        <p className="sar-empty"><AlertTriangle size={14} aria-hidden /> {data.reason}</p>
      ) : null}

      {data?.available && stats && baseline ? (
        <>
          <div className="sar-stats">
            <div>
              <span>Win rate</span>
              <strong>{stats.win_rate === null ? "—" : `${stats.win_rate}%`}</strong>
              <Delta stats={stats} baseline={baseline} />
            </div>
            <div><span>Signals</span><strong>{stats.count.toLocaleString("en-IN")}</strong></div>
            <div><span>Won / lost / flat</span><strong>{stats.wins} / {stats.losses} / {stats.timeouts}</strong></div>
            <div><span>Average outcome</span><strong>{pct(stats.avg_final_pct, 2)}</strong></div>
            <div><span>Median outcome</span><strong>{pct(stats.median_final_pct, 2)}</strong></div>
            <div><span>Average best moment</span><strong>{pct(stats.avg_max_favourable_pct, 2)}</strong></div>
          </div>
          <p className="sar-baseline">
            Baseline for comparison is {data.baseline_label}: {baseline.win_rate ?? "—"}% win rate over{" "}
            {baseline.count.toLocaleString("en-IN")} signals. These are counted outcomes of past signals
            under a 3% stop and a 5% target over ten sessions — a measurement of what happened, not a
            forecast. {data.hidden_from_todays_drill > 0 ? (
              <>{data.hidden_from_todays_drill} cards are held back because they are in today&apos;s Chart Gym hand.</>
            ) : null}
          </p>

          <div className="sar-table" role="table">
            <div className="sar-row sar-row--head" role="row">
              <span role="columnheader">Signal</span>
              <span role="columnheader">Triggered</span>
              <span role="columnheader">RS</span>
              <span role="columnheader">Risk</span>
              <span role="columnheader">Best</span>
              <span role="columnheader">Outcome</span>
            </div>
            {loading && data.rows.length === 0 ? <p className="sar-empty">Loading…</p> : null}
            {!loading && data.rows.length === 0 ? (
              <p className="sar-empty">No signals match that combination.</p>
            ) : null}
            {data.rows.map((row) => (
              <button
                key={row.id}
                type="button"
                className={`sar-row sar-row--${row.result}`}
                role="row"
                onClick={() => onOpenSymbolChart?.(row.symbol)}
                title={row.reasons.join(" · ")}
              >
                <span role="cell" className="sar-sig">
                  <strong>{row.symbol}</strong>
                  <em>{row.label}{row.group_top_decile ? " · top-decile group" : ""}</em>
                  {row.reasons.length ? <small>{row.reasons.join(" · ")}</small> : null}
                </span>
                <span role="cell">{row.trigger_date}</span>
                <span role="cell">{row.rs_rating || "—"}</span>
                <span role="cell">{row.scanner_risk_pct === null ? "—" : `${row.scanner_risk_pct.toFixed(1)}%`}</span>
                <span role="cell">{pct(row.max_favourable_pct)}</span>
                <span role="cell" className="sar-outcome">
                  <strong>{pct(row.final_pct)}</strong>
                  <em>{row.result === "timeout" ? "went nowhere" : row.result}</em>
                </span>
              </button>
            ))}
          </div>

          {pages > 1 ? (
            <div className="sar-pager">
              <button
                type="button"
                disabled={data.offset <= 0 || loading}
                onClick={() => setQuery((prev) => ({ ...prev, offset: Math.max(0, (prev.offset ?? 0) - (prev.limit ?? 50)) }))}
              >
                Previous
              </button>
              <span>Page {page} of {pages}</span>
              <button
                type="button"
                disabled={page >= pages || loading}
                onClick={() => setQuery((prev) => ({ ...prev, offset: (prev.offset ?? 0) + (prev.limit ?? 50) }))}
              >
                Next
              </button>
            </div>
          ) : null}
        </>
      ) : null}
    </section>
  );
}
