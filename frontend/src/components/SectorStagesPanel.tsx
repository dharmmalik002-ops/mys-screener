import { useEffect, useMemo, useState } from "react";
import {
  getMfSectorSeries,
  getMfSectorStages,
  type MfSectorBucket,
  type MfSectorRow,
  type MfSectorSeries,
  type MfSectorStages,
} from "../lib/api";
import { SectorChart, type SectorCandlePeriod, type SectorChartMode } from "./SectorChart";

import "./SectorStagesPanel.css";

/**
 * Where each Nifty sector sits in its own price cycle.
 *
 * Weinstein stage analysis: a sector is basing (1), advancing (2), topping (3)
 * or declining (4), decided by price against a 30-week average and the slope of
 * that average. The page groups them the way the question is usually asked —
 * what has turned, what is working, what is going nowhere.
 *
 * **These are measurements, not calls.** A base that is tightening with
 * improving relative strength is measurably further along than one that is
 * not, and that is all `readiness` scores. Bases fail. Nothing here says to buy
 * a sector or a fund; the funds listed under each sector are the ones this app
 * already benchmarks against that index, which is a mapping, not a shortlist.
 */

const SECTIONS: {
  key: MfSectorBucket;
  title: string;
  blurb: string;
  tone: string;
}[] = [
  {
    key: "turning",
    title: "Finished falling, base well formed",
    tone: "is-turning",
    blurb:
      "The decline has stopped and the base has tightened far enough that the setup is visible — a flattening 30-week average, a narrowing range, price in the upper part of it. This is the Stage 1 to Stage 2 transition. It is where an advance would begin if one begins; plenty of bases never resolve upward.",
  },
  {
    key: "advancing",
    title: "Already advancing",
    tone: "is-advancing",
    blurb:
      "Price above a rising 30-week average — Stage 2, the markup phase. These sectors are already in the move rather than waiting to start one.",
  },
  {
    key: "sideways",
    title: "Going sideways",
    tone: "is-sideways",
    blurb:
      "Range-bound: either a base that has stopped falling but has not tightened yet, or an advance that has stalled and is chopping across a flat average. Direction is genuinely unsettled here.",
  },
  {
    key: "declining",
    title: "Still declining",
    tone: "is-declining",
    blurb: "Price below a falling 30-week average. The downtrend is still in force.",
  },
];

const RANGES = ["6m", "1y", "2y", "3y", "5y"];

const pct = (value: number | null | undefined, digits = 1): string =>
  typeof value === "number" && Number.isFinite(value)
    ? `${value > 0 ? "+" : value < 0 ? "−" : ""}${Math.abs(value).toFixed(digits)}%`
    : "—";

/** Stage distribution across all sectors — how broad the market's phase is. */
function StageDonut({ counts }: { counts: Record<MfSectorBucket, number> }) {
  const slices = SECTIONS.map((section) => ({
    key: section.key,
    label: section.title,
    value: counts[section.key] ?? 0,
  })).filter((slice) => slice.value > 0);

  const total = slices.reduce((sum, slice) => sum + slice.value, 0) || 1;
  const size = 132;
  const thickness = 15;
  const radius = (size - thickness - 4) / 2;
  const circumference = 2 * Math.PI * radius;
  let offset = 0;

  return (
    <div className="ssp-donut-card">
      <h4>All {total} sectors by phase</h4>
      <div className="ssp-donut-row">
        <svg viewBox={`0 0 ${size} ${size}`} width={size} height={size} className="ssp-donut"
             role="img" aria-label="Sectors grouped by market phase">
          <g transform={`translate(${size / 2},${size / 2}) rotate(-90)`}>
            {slices.map((slice) => {
              const length = (slice.value / total) * circumference;
              const el = (
                <circle
                  key={slice.key}
                  r={radius}
                  fill="none"
                  className={`ssp-arc ssp-arc-${slice.key}`}
                  strokeWidth={thickness}
                  strokeDasharray={`${Math.max(0, length - 1.5)} ${circumference}`}
                  strokeDashoffset={-offset}
                />
              );
              offset += length;
              return el;
            })}
          </g>
        </svg>
        <ul className="ssp-donut-legend">
          {slices.map((slice) => (
            <li key={slice.key}>
              <i className={`ssp-swatch ssp-arc-${slice.key}`} />
              <span>{slice.label}</span>
              <b>{slice.value}</b>
            </li>
          ))}
        </ul>
      </div>
      <p className="ssp-note">
        How broad the market's phase is. A market where most sectors are advancing behaves very
        differently from one where most are basing — and this is that count, nothing more.
      </p>
    </div>
  );
}

/** Readiness as a filled meter — only meaningful for a base. */
function ReadinessMeter({ value }: { value: number }) {
  return (
    <div className="ssp-meter" title={`${value} of 100 — how fully the base has formed`}>
      <span className="ssp-meter-track">
        <i style={{ width: `${value}%` }} className={value >= 55 ? "is-ready" : ""} />
      </span>
      <b>{value}</b>
    </div>
  );
}

function SectorCard({ row, onOpen }: { row: MfSectorRow; onOpen: (row: MfSectorRow) => void }) {
  return (
    <li className={`ssp-card stage-${row.stage}`}>
      <button type="button" className="ssp-card-btn" onClick={() => onOpen(row)}>
        <div className="ssp-card-head">
          <span className="ssp-card-name">{row.name}</span>
          <span className={`ssp-stage-chip stage-${row.stage}`}>Stage {row.stage}</span>
        </div>

        <div className="ssp-card-metrics">
          <span>
            <em>13 weeks</em>
            <b className={(row.return_13w_pct ?? 0) < 0 ? "is-down" : "is-up"}>
              {pct(row.return_13w_pct)}
            </b>
          </span>
          <span>
            <em>vs 30w avg</em>
            <b className={row.distance_from_ma_pct < 0 ? "is-down" : "is-up"}>
              {pct(row.distance_from_ma_pct)}
            </b>
          </span>
          <span>
            <em>avg slope</em>
            <b className={row.ma_slope_pct_per_week < 0 ? "is-down" : "is-up"}>
              {pct(row.ma_slope_pct_per_week, 2)}/wk
            </b>
          </span>
          {row.rs_13w_vs_market_pct != null ? (
            <span>
              <em>vs market</em>
              <b className={row.rs_13w_vs_market_pct < 0 ? "is-down" : "is-up"}>
                {pct(row.rs_13w_vs_market_pct)}
              </b>
            </span>
          ) : null}
        </div>

        {row.stage === 1 ? <ReadinessMeter value={row.readiness} /> : null}

        <p className="ssp-card-summary">{row.summary}</p>

        <div className="ssp-card-foot">
          {row.fund_count ? <i>{row.fund_count} funds track this</i> : <i>no funds map here</i>}
          {row.is_stale ? (
            <i className="ssp-stale" title={`Last index bar ${row.as_of}`}>
              data {row.days_behind}d behind
            </i>
          ) : null}
          <i className="ssp-open">chart →</i>
        </div>
      </button>
    </li>
  );
}

function SectorModal({ row, onClose }: { row: MfSectorRow; onClose: () => void }) {
  const [series, setSeries] = useState<MfSectorSeries | null>(null);
  const [loading, setLoading] = useState(true);
  const [mode, setMode] = useState<SectorChartMode>("line");
  const [period, setPeriod] = useState<SectorCandlePeriod>("weekly");
  const [range, setRange] = useState("3y");

  useEffect(() => {
    let cancelled = false;
    setLoading(true);
    getMfSectorSeries(row.key, range)
      .then((payload) => { if (!cancelled) setSeries(payload); })
      .catch(() => { if (!cancelled) setSeries(null); })
      .finally(() => { if (!cancelled) setLoading(false); });
    return () => { cancelled = true; };
  }, [row.key, range]);

  useEffect(() => {
    const onKey = (event: KeyboardEvent) => { if (event.key === "Escape") onClose(); };
    window.addEventListener("keydown", onKey);
    return () => window.removeEventListener("keydown", onKey);
  }, [onClose]);

  return (
    <div className="ssp-modal-backdrop" onClick={onClose} role="presentation">
      <div className="ssp-modal" onClick={(event) => event.stopPropagation()} role="dialog" aria-label={row.name}>
        <header className="ssp-modal-head">
          <div>
            <h3>{row.name}</h3>
            <p>
              <span className={`ssp-stage-chip stage-${row.stage}`}>
                Stage {row.stage} · {row.stage_label}
              </span>
              <span className="ssp-modal-symbol">{row.symbol}</span>
              {row.is_stale ? (
                <span className="ssp-stale">last bar {row.as_of} · {row.days_behind} days behind</span>
              ) : null}
            </p>
          </div>
          <button type="button" className="ssp-modal-close" onClick={onClose} aria-label="Close">×</button>
        </header>

        <div className="ssp-modal-controls">
          <div className="ssp-seg">
            {RANGES.map((key) => (
              <button key={key} type="button" className={range === key ? "active" : ""} onClick={() => setRange(key)}>
                {key.toUpperCase()}
              </button>
            ))}
          </div>
          <div className="ssp-seg">
            <button type="button" className={mode === "line" ? "active" : ""} onClick={() => setMode("line")}>
              Line
            </button>
            <button type="button" className={mode === "candles" ? "active" : ""} onClick={() => setMode("candles")}>
              Candles
            </button>
          </div>
          {mode === "candles" ? (
            <div className="ssp-seg">
              <button type="button" className={period === "daily" ? "active" : ""} onClick={() => setPeriod("daily")}>
                Daily
              </button>
              <button type="button" className={period === "weekly" ? "active" : ""} onClick={() => setPeriod("weekly")}>
                Weekly
              </button>
            </div>
          ) : null}
        </div>

        {loading ? (
          <div className="ssp-modal-placeholder">Loading {row.name}…</div>
        ) : series ? (
          <>
            <SectorChart series={series} mode={mode} period={period} height={320} />
            <p className="ssp-modal-foot">
              The dashed line is the 30-week average the stage is decided on.{" "}
              {mode === "candles" && period === "weekly"
                ? "Weekly candles, which is the timeframe stage analysis is built on."
                : mode === "candles"
                  ? "Daily candles from the index feed — real open, high, low and close."
                  : "Closing prices."}{" "}
              This is a price index: it excludes dividends.
            </p>
          </>
        ) : (
          <div className="ssp-modal-placeholder">Could not load this index.</div>
        )}

        <p className="ssp-modal-summary">{row.summary}</p>

        {row.funds?.length ? (
          <div className="ssp-modal-funds">
            <h4>Funds benchmarked to this index</h4>
            <p className="ssp-note">
              The funds this app already measures against {row.name}, largest first. A mapping of
              what tracks what — not a shortlist.
            </p>
            <table className="ssp-fund-table">
              <thead>
                <tr>
                  <th className="is-left">Fund</th><th>1Y</th><th>3Y</th><th>Cost</th><th>AUM</th>
                </tr>
              </thead>
              <tbody>
                {row.funds.map((fund) => (
                  <tr key={fund.scheme_code}>
                    <td className="is-left">{fund.name}</td>
                    <td>{pct(fund.return_1y)}</td>
                    <td>{pct(fund.return_3y)}</td>
                    <td>{fund.expense_ratio != null ? `${fund.expense_ratio.toFixed(2)}%` : "—"}</td>
                    <td>{fund.aum_crore != null ? `₹${Math.round(fund.aum_crore).toLocaleString("en-IN")} cr` : "—"}</td>
                  </tr>
                ))}
              </tbody>
            </table>
            {row.fund_count && row.fund_count > row.funds.length ? (
              <p className="ssp-note">
                and {row.fund_count - row.funds.length} more — search the screener for this theme.
              </p>
            ) : null}
          </div>
        ) : null}
      </div>
    </div>
  );
}

export function SectorStagesPanel() {
  const [data, setData] = useState<MfSectorStages | null>(null);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState<string | null>(null);
  const [open, setOpen] = useState<MfSectorRow | null>(null);

  useEffect(() => {
    let cancelled = false;
    getMfSectorStages()
      .then((payload) => { if (!cancelled) { setData(payload); setError(null); } })
      .catch((err) => { if (!cancelled) setError(err instanceof Error ? err.message : "Could not load sectors."); })
      .finally(() => { if (!cancelled) setLoading(false); });
    return () => { cancelled = true; };
  }, []);

  const sections = useMemo(
    () => SECTIONS.map((section) => ({ ...section, rows: data?.buckets?.[section.key] ?? [] })),
    [data],
  );

  if (loading) return <div className="ssp-loading">Reading sixteen sector indices…</div>;
  if (error || !data) return <div className="ssp-loading">{error ?? "No sector data."}</div>;

  return (
    <div className="ssp">
      <header className="ssp-head">
        <div>
          <h2>Sector cycles</h2>
          <p>
            Each Nifty sector index placed in its own price cycle, using Weinstein stage analysis:
            price against a 30-week average, and the slope of that average. Sectors are grouped by
            phase — what has finished falling, what is already running, what is going nowhere.
            These are measurements of what price has done. A base can take months and some never
            resolve upward, so nothing here is a forecast or a recommendation.
          </p>
        </div>
        <span className="ssp-asof">
          weekly closes to {data.as_of ?? "—"}
        </span>
      </header>

      {data.stale_count ? (
        <p className="ssp-warn">
          The index feed has not updated {data.stale_count} of {data.sector_count} sector indices
          since well before {data.as_of}. Those cards are marked with how far behind they are —
          their stage is read from the last bars available, not from this week.
        </p>
      ) : null}

      <StageDonut counts={data.counts} />

      {sections.map((section) => (
        <section key={section.key} className={`ssp-section ${section.tone}`}>
          <header>
            <h3>
              {section.title}
              <span className="ssp-count">{section.rows.length}</span>
            </h3>
            <p>{section.blurb}</p>
          </header>
          {section.rows.length ? (
            <ul className="ssp-grid">
              {section.rows.map((row) => (
                <SectorCard key={row.key} row={row} onOpen={setOpen} />
              ))}
            </ul>
          ) : (
            <p className="ssp-empty">No sector is in this phase right now.</p>
          )}
        </section>
      ))}

      <p className="ssp-method">
        Stage is decided on weekly closes against a {data.method.average_weeks}-week average. An
        average moving less than {data.method.flat_slope_pct}% a week counts as flat; a base is
        called tight inside {data.method.tight_range_pct}% of price. The readiness meter on a base
        scores how fully the setup has formed out of 100 — a flattening average, price reclaiming
        it, a narrowing range, and relative strength turning up — and reaches this page's first
        section at {data.method.readiness_threshold}. It is a description of the chart, not a
        probability that the base resolves upward.
      </p>

      {open ? <SectorModal row={open} onClose={() => setOpen(null)} /> : null}
    </div>
  );
}
