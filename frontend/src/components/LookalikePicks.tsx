import { useEffect, useMemo, useState } from "react";
import { ChevronLeft, ChevronRight, ExternalLink } from "lucide-react";

import {
  getLookalikePicksForDay,
  type LookalikePick,
  type LookalikePicksSummary,
  type LookalikeRefRow,
} from "../lib/api";
import { fullChartUrl } from "../lib/chartLink";
import { LookalikeChart } from "./LookalikeChart";
import { CompareModal, RefOutcome } from "./LookalikeModals";

/* The pick calendar and the review / learning views of the Look-alikes page.

   Every pick is shown with its outcome next to its reason, so a reason that
   sounded convincing and then failed is visible as exactly that. Backfilled
   days are labelled: they are honest tests (the library ends in 2022) but they
   were made in one go, and the stock list is today's, so delisted names that
   would have been picked and failed are missing from them. */

type Summary = Extract<LookalikePicksSummary, { available: true }>;

const WEEKDAYS = ["Mon", "Tue", "Wed", "Thu", "Fri", "Sat", "Sun"];

function iso(d: Date) {
  const y = d.getFullYear();
  const m = String(d.getMonth() + 1).padStart(2, "0");
  const day = String(d.getDate()).padStart(2, "0");
  return `${y}-${m}-${day}`;
}

function formatDate(isoDate: string | undefined | null) {
  if (!isoDate) return "—";
  const d = new Date(`${isoDate}T00:00:00`);
  return Number.isNaN(d.getTime()) ? isoDate : d.toLocaleDateString("en-IN", { day: "numeric", month: "short", year: "numeric" });
}

export function OutcomeChip({ outcome }: { outcome: LookalikePick["outcome"] }) {
  const label = outcome?.label ?? "pending";
  const text =
    label === "worked"
      ? `Worked · +${(outcome.max_gain_pct ?? 0).toFixed(1)}%${outcome.days_to_result ? ` in ${outcome.days_to_result} session${outcome.days_to_result === 1 ? "" : "s"}` : ""}`
      : label === "failed"
        ? `Failed · ${(outcome.max_loss_pct ?? 0).toFixed(1)}%${outcome.days_to_result ? ` in ${outcome.days_to_result} session${outcome.days_to_result === 1 ? "" : "s"}` : ""}`
        : outcome?.sessions_observed
          ? `Open · best +${(outcome.max_gain_pct ?? 0).toFixed(1)}%, worst ${(outcome.max_loss_pct ?? 0).toFixed(1)}% after ${outcome.sessions_observed}d`
          : "Open · not reviewed yet";
  return <span className={`lookalike-chip is-${label}`}>{text}</span>;
}

function PickCard({ pick, refs }: { pick: LookalikePick; refs: Record<string, LookalikeRefRow> }) {
  const [pickIdx, setPickIdx] = useState(0);
  const [compare, setCompare] = useState<LookalikeRefRow | null>(null);
  const nearest = pick.nearest ?? [];
  const near = nearest[Math.min(pickIdx, nearest.length - 1)];
  const ref = near?.key ? refs[near.key] : undefined;
  return (
    <article className="lookalike-pick">
      <header className="lookalike-card-head">
        <span className="lookalike-rank">#{pick.rank}</span>
        <a className="lookalike-symbol" href={fullChartUrl(pick.symbol)} target="_blank" rel="noreferrer noopener" title="Open on my site in a new tab">
          {pick.symbol}
        </a>
        <span className="lookalike-meta">
          ₹{pick.close.toLocaleString("en-IN")} · beats {pick.percentile.toFixed(0)}% of ordinary charts · Trend Template{" "}
          {pick.template ?? "—"}/8
        </span>
        <OutcomeChip outcome={pick.outcome} />
      </header>
      <div className="lookalike-pair">
        <figure>
          <LookalikeChart data={pick.chart} height={160} labels ariaLabel={`${pick.symbol} at ${pick.session}`} />
          <figcaption>
            {pick.symbol} on {formatDate(pick.session)}
          </figcaption>
        </figure>
        <figure>
          {ref ? (
            <button type="button" className="lookalike-chart-button" onClick={() => setCompare(ref)} title="Compare large, at its own date">
              <LookalikeChart data={ref.chart} height={160} labels ariaLabel={`${ref.ticker} at ${ref.date}`} />
            </button>
          ) : (
            <div className="lookalike-canvas lookalike-missing" style={{ height: 160 }}>
              Example unavailable
            </div>
          )}
          <figcaption>
            {near ? (
              <>
                <span>
                  {near.name} · {(near.similarity * 100).toFixed(0)}% alike
                </span>
                {ref ? <RefOutcome ref={ref} /> : null}
              </>
            ) : (
              "—"
            )}
          </figcaption>
        </figure>
      </div>
      {nearest.length > 1 ? (
        <div className="lookalike-nearest" role="tablist" aria-label="Closest examples">
          {nearest.map((n, i) => (
            <button
              key={`${n.ticker}-${n.date}`}
              type="button"
              role="tab"
              aria-selected={i === pickIdx}
              className={`lookalike-near-tab${i === pickIdx ? " is-active" : ""}`}
              onClick={() => setPickIdx(i)}
            >
              {i + 1}. {n.ticker} · {(n.similarity * 100).toFixed(0)}%
            </button>
          ))}
        </div>
      ) : null}
      <details className="lookalike-why">
        <summary>Why it was chosen</summary>
        <p className="lookalike-reason">{pick.reason}</p>
      </details>
      <div className="lookalike-links">
        <a className="lookalike-link" href={fullChartUrl(pick.symbol)} target="_blank" rel="noreferrer noopener">
          Open {pick.symbol} on my site <ExternalLink size={12} />
        </a>
        {ref ? (
          <button type="button" className="lookalike-link" onClick={() => setCompare(ref)}>
            Compare with {ref.ticker} at {formatDate(ref.date)}
          </button>
        ) : null}
        {pick.source === "backfill" ? <span className="lookalike-tag">backfilled</span> : null}
        {pick.ranked_by === "learned_outcome" ? <span className="lookalike-tag">ranked by what has worked</span> : null}
      </div>
      {compare ? (
        <CompareModal
          symbol={pick.symbol}
          chart={pick.chart}
          session={pick.session}
          reference={compare}
          onClose={() => setCompare(null)}
          vote={{
            query: pick.symbol,
            session: pick.session,
            kind: "ref",
            target: `${compare.style}:${compare.ticker}@${compare.date}`,
            style: compare.style,
          }}
        />
      ) : null}
    </article>
  );
}

export function CalendarView({ summary }: { summary: Summary; onOpen?: (symbol: string) => void }) {
  const days = useMemo(() => Object.keys(summary.calendar ?? {}).sort(), [summary]);
  const lastDay = days[days.length - 1] ?? iso(new Date());
  const [selected, setSelected] = useState<string>(lastDay);
  const [month, setMonth] = useState<Date>(() => {
    const d = new Date(`${lastDay}T00:00:00`);
    return new Date(d.getFullYear(), d.getMonth(), 1);
  });
  const [dayPicks, setDayPicks] = useState<LookalikePick[] | null>(null);
  const [dayRefs, setDayRefs] = useState<Record<string, LookalikeRefRow>>({});
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState<string | null>(null);

  useEffect(() => {
    let cancelled = false;
    setLoading(true);
    setError(null);
    getLookalikePicksForDay(selected)
      .then((res) => {
        if (!cancelled) {
          setDayPicks(res?.picks ?? []);
          setDayRefs(res?.refs ?? {});
        }
      })
      .catch((err) => {
        if (!cancelled) setError(err instanceof Error ? err.message : "Could not load that day's picks.");
      })
      .finally(() => {
        if (!cancelled) setLoading(false);
      });
    return () => {
      cancelled = true;
    };
  }, [selected]);

  const cells = useMemo(() => {
    const first = new Date(month.getFullYear(), month.getMonth(), 1);
    const offset = (first.getDay() + 6) % 7; // Monday first
    const count = new Date(month.getFullYear(), month.getMonth() + 1, 0).getDate();
    const out: Array<string | null> = Array(offset).fill(null);
    for (let d = 1; d <= count; d += 1) out.push(iso(new Date(month.getFullYear(), month.getMonth(), d)));
    while (out.length % 7) out.push(null);
    return out;
  }, [month]);

  const firstMonth = days.length ? new Date(`${days[0]}T00:00:00`) : null;
  const canPrev = !firstMonth || month > new Date(firstMonth.getFullYear(), firstMonth.getMonth(), 1);
  const canNext = month < new Date(new Date().getFullYear(), new Date().getMonth(), 1);
  const shift = (n: number) => setMonth((m) => new Date(m.getFullYear(), m.getMonth() + n, 1));

  return (
    <div className="lookalike-calendar-layout">
      <section className="lookalike-calendar" aria-label="Pick calendar">
        <header className="lookalike-calendar-head">
          <button type="button" className="lookalike-refresh" onClick={() => shift(-1)} disabled={!canPrev} aria-label="Previous month">
            <ChevronLeft size={14} />
          </button>
          <strong>{month.toLocaleDateString("en-IN", { month: "long", year: "numeric" })}</strong>
          <button type="button" className="lookalike-refresh" onClick={() => shift(1)} disabled={!canNext} aria-label="Next month">
            <ChevronRight size={14} />
          </button>
        </header>
        <div className="lookalike-calendar-grid" role="grid">
          {WEEKDAYS.map((w) => (
            <span key={w} className="lookalike-calendar-weekday">
              {w}
            </span>
          ))}
          {cells.map((day, i) => {
            if (!day) return <span key={`blank-${i}`} />;
            const info = summary.calendar?.[day];
            const cls = [
              "lookalike-calendar-day",
              info ? "has-picks" : "",
              day === selected ? "is-selected" : "",
              info && info.worked + info.failed > 0
                ? info.worked >= info.failed
                  ? "lean-worked"
                  : "lean-failed"
                : "",
            ].join(" ");
            return (
              <button
                key={day}
                type="button"
                className={cls}
                onClick={() => setSelected(day)}
                title={info ? `${info.picks} picks · ${info.worked} worked · ${info.failed} failed · ${info.pending} open` : "No picks"}
              >
                <span>{Number(day.slice(8))}</span>
                {info ? <small>{info.picks}</small> : null}
              </button>
            );
          })}
        </div>
        <p className="lookalike-stat-sub">
          Numbers are how many charts were picked that day. Green: more worked than failed; red: more failed. Past
          Fridays since Jan 2024 were backfilled; from now on every scan day is recorded.
        </p>
      </section>

      <section className="lookalike-day" aria-live="polite">
        <h3>Picks for {formatDate(selected)}</h3>
        {loading ? <p className="lookalike-empty">Loading…</p> : null}
        {error ? <p className="lookalike-empty">{error}</p> : null}
        {!loading && !error && dayPicks && dayPicks.length === 0 ? (
          <p className="lookalike-empty">
            No picks on this date. Scans run on trading days; past dates were only backfilled on Fridays.
          </p>
        ) : null}
        {!loading && dayPicks?.length ? (
          <div className="lookalike-day-grid">
            {dayPicks.map((pick) => (
              <PickCard key={pick.id} pick={pick} refs={dayRefs} />
            ))}
          </div>
        ) : null}
      </section>
    </div>
  );
}

export function ReviewsView({ summary }: { summary: Summary }) {
  const s = summary.summary;
  const learn = summary.learning;
  const pct = (v: number | null | undefined) => (v == null ? "—" : `${v.toFixed(0)}%`);
  const [span, setSpan] = useState<"week" | "fortnight">("week");

  const rows = useMemo(() => {
    const weekly = summary.weekly ?? [];
    if (span === "week") return weekly;
    // Pair consecutive weeks into fortnights, newest first.
    const out: typeof weekly = [];
    for (let i = 0; i < weekly.length; i += 2) {
      const pair = weekly.slice(i, i + 2);
      const worked = pair.reduce((a, r) => a + r.worked, 0);
      const failed = pair.reduce((a, r) => a + r.failed, 0);
      out.push({
        week_of: pair[pair.length - 1].week_of,
        picks: pair.reduce((a, r) => a + r.picks, 0),
        worked,
        failed,
        pending: pair.reduce((a, r) => a + r.pending, 0),
        worked_pct: worked + failed ? (100 * worked) / (worked + failed) : null,
        best: pair.find((r) => r.best)?.best ?? null,
      });
    }
    return out;
  }, [summary, span]);

  return (
    <div className="lookalike-reviews">
      <section className="lookalike-summary">
        <div className="lookalike-stat">
          <span className="lookalike-stat-label">Picks made</span>
          <strong>{s.picks.toLocaleString("en-IN")}</strong>
          <span className="lookalike-stat-sub">on {s.days} days</span>
        </div>
        <div className="lookalike-stat">
          <span className="lookalike-stat-label">Finished picks that worked</span>
          <strong>
            {pct(s.worked_pct)}
            <span className="lookalike-stat-vs"> of {s.decided}</span>
          </strong>
          <span className="lookalike-stat-sub">
            +{summary.outcome_rule.target_pct}% before −{summary.outcome_rule.stop_pct}% within {summary.outcome_rule.horizon_sessions}{" "}
            sessions · his US setups did this {pct(s.style_base_rate_pct)} of the time
          </span>
        </div>
        <div className="lookalike-stat">
          <span className="lookalike-stat-label">Backfilled vs live</span>
          <strong>
            {pct(s.by_source?.backfill?.worked_pct)} <span className="lookalike-stat-vs">vs</span> {pct(s.by_source?.live?.worked_pct)}
          </strong>
          <span className="lookalike-stat-sub">
            {s.by_source?.backfill?.decided ?? 0} backfilled · {s.by_source?.live?.decided ?? 0} live picks finished
          </span>
        </div>
        <div className="lookalike-stat">
          <span className="lookalike-stat-label">Learning from results</span>
          <strong>{learn.in_use ? "In use" : "Watching"}</strong>
          <span className="lookalike-stat-sub">{learn.status}</span>
        </div>
      </section>

      {summary.baselines ? (
        <section className="lookalike-curve-wrap">
          <h3>Are the picks better than the alternatives?</h3>
          <p className="lookalike-stat-sub">
            Every stock on the same days, graded by the same rule. If the picks do not clear these, the picture matching
            is not adding anything.
          </p>
          <div className="lookalike-rules-table-wrap">
            <table className="lookalike-rules-table">
              <thead>
                <tr>
                  <th>Choosing</th>
                  <th>Worked</th>
                  <th>Charts</th>
                </tr>
              </thead>
              <tbody>
                <tr>
                  <td>The system's picks (look like his setups + all 8 rules)</td>
                  <td>
                    {pct(summary.baselines.picks?.worked_pct)}
                    {summary.baselines.picks?.margin_pct != null ? ` ± ${summary.baselines.picks.margin_pct.toFixed(1)}` : ""}
                  </td>
                  <td>{summary.baselines.picks?.charts?.toLocaleString("en-IN")}</td>
                </tr>
                <tr>
                  <td>Any stock passing all 8 Trend Template rules</td>
                  <td>{pct(summary.baselines.template8?.worked_pct)}</td>
                  <td>{summary.baselines.template8?.charts?.toLocaleString("en-IN")}</td>
                </tr>
                <tr>
                  <td>Any stock at all</td>
                  <td>{pct(summary.baselines.all?.worked_pct)}</td>
                  <td>{summary.baselines.all?.charts?.toLocaleString("en-IN")}</td>
                </tr>
                {summary.baselines.learned_ranked?.charts ? (
                  <tr>
                    <td>Picks made while the learner was ranking them</td>
                    <td>{pct(summary.baselines.learned_ranked.worked_pct)}</td>
                    <td>{summary.baselines.learned_ranked.charts}</td>
                  </tr>
                ) : null}
              </tbody>
            </table>
          </div>
        </section>
      ) : null}

      <section className="lookalike-curve-wrap">
        <h3>What separated winners from losers</h3>
        <p className="lookalike-stat-sub">
          Among finished picks only. A row needs at least 20 picks on each side before it is a lesson rather than a
          coincidence.
        </p>
        <div className="lookalike-rules-table-wrap">
          <table className="lookalike-rules-table">
            <thead>
              <tr>
                <th>Condition</th>
                <th>Worked with it</th>
                <th>Worked without it</th>
                <th>Picks (with / without)</th>
              </tr>
            </thead>
            <tbody>
              {(summary.lessons ?? []).map((row) => (
                <tr key={row.condition} className={row.enough ? "" : "is-thin"}>
                  <td>{row.condition}</td>
                  <td>{row.enough ? pct(row.worked_with_pct) : "—"}</td>
                  <td>{row.enough ? pct(row.worked_without_pct) : "—"}</td>
                  <td>
                    {row.with} / {row.without}
                  </td>
                </tr>
              ))}
            </tbody>
          </table>
        </div>
      </section>

      <section className="lookalike-curve-wrap">
        <div className="lookalike-toolbar">
          <h3 style={{ margin: 0 }}>Review by {span}</h3>
          {(["week", "fortnight"] as const).map((opt) => (
            <button
              key={opt}
              type="button"
              className={`lookalike-filter${span === opt ? " is-active" : ""}`}
              onClick={() => setSpan(opt)}
            >
              {opt === "week" ? "Weekly" : "Fortnightly"}
            </button>
          ))}
        </div>
        <div className="lookalike-rules-table-wrap">
          <table className="lookalike-rules-table">
            <thead>
              <tr>
                <th>{span === "week" ? "Week of" : "Fortnight from"}</th>
                <th>Picks</th>
                <th>Worked</th>
                <th>Failed</th>
                <th>Still open</th>
                <th>Worked %</th>
                <th>Best move</th>
              </tr>
            </thead>
            <tbody>
              {rows.map((row) => (
                <tr key={row.week_of}>
                  <td>{formatDate(row.week_of)}</td>
                  <td>{row.picks}</td>
                  <td>{row.worked}</td>
                  <td>{row.failed}</td>
                  <td>{row.pending}</td>
                  <td>{pct(row.worked_pct)}</td>
                  <td>{row.best ?? "—"}</td>
                </tr>
              ))}
            </tbody>
          </table>
        </div>
      </section>
    </div>
  );
}
