import { useCallback, useEffect, useMemo, useRef, useState } from "react";
import { AlertTriangle, Check, RefreshCw, Sparkles } from "lucide-react";
import {
  getMacroContext,
  type MacroContext,
  type MacroSeriesRow,
  type MarketKey,
} from "../lib/api";
import "./MacroContextPanel.css";

type Props = { market: MarketKey };

const STANCE_LABEL = {
  supportive: "Wind at your back",
  mixed: "Neutral backdrop",
  hostile: "Wind in your face",
} as const;

/** Which series belong under which section's "show the numbers" drawer. The
 *  mapping lives here rather than on the server because it is presentation:
 *  the prose is complete without it, and the evidence is an affordance for a
 *  reader who wants to check a sentence. */
const SECTION_EVIDENCE: Record<string, string[]> = {
  overnight: ["sp500", "nasdaq", "nikkei", "hangseng", "kospi", "ftse", "dax"],
  macro: ["brent", "dxy", "usdinr", "ust10y", "gold", "indiavix"],
  linkage: ["nifty", "sp500"],
};

function pct(value: number | null | undefined, digits = 1): string {
  if (value === null || value === undefined) return "—";
  return `${value > 0 ? "+" : ""}${value.toFixed(digits)}%`;
}

/** Green means "good for Indian equities", not "the number went up".
 *
 *  Colouring by sign put a green +18.8% on crude directly beside prose calling
 *  it the market's biggest headwind — teaching the reader the opposite of the
 *  point the paragraph was making. */
function tone(value: number | null | undefined, effect: MacroSeriesRow["effect"] = "up_helps"): string {
  if (value === null || value === undefined) return "mcx-flat";
  if (Math.abs(value) <= 0.05 || effect === "neutral") return "mcx-flat";
  const helps = effect === "up_helps" ? value > 0 : value < 0;
  return helps ? "mcx-pos" : "mcx-neg";
}

/** Yields are quoted to two places: `toLocaleString` alone renders 5.00 as "5",
 *  which does not read as a rate. */
function level(row: MacroSeriesRow): string {
  if (row.last === null) return "—";
  const digits = row.unit === "pct" ? 2 : 0;
  return row.last.toLocaleString(undefined, {
    minimumFractionDigits: digits,
    maximumFractionDigits: 2,
  });
}

function EvidenceRows({ rows }: { rows: MacroSeriesRow[] }) {
  if (!rows.length) return null;
  return (
    <div className="mcx-evidence" role="table" aria-label="Underlying numbers">
      <div className="mcx-evidence-head" role="row">
        <span role="columnheader">Instrument</span>
        <span role="columnheader">Last</span>
        <span role="columnheader">1 day</span>
        <span role="columnheader">1 month</span>
        <span role="columnheader">vs 50-DMA</span>
      </div>
      <p className="mcx-evidence-key">Green means good for Indian equities, not "went up".</p>
      {rows.map((row) => (
        <div className="mcx-evidence-row" role="row" key={row.key}>
          <span role="cell">
            <strong>{row.label}</strong>
            <em>{row.blurb}</em>
          </span>
          <span role="cell">{level(row)}</span>
          <span role="cell" className={tone(row.change_1d_pct, row.effect)}>{pct(row.change_1d_pct)}</span>
          <span role="cell" className={tone(row.change_20d_pct, row.effect)}>{pct(row.change_20d_pct)}</span>
          <span role="cell" className={tone(row.vs_50dma_pct, row.effect)}>{pct(row.vs_50dma_pct)}</span>
        </div>
      ))}
    </div>
  );
}

function FlowEvidence({ data }: { data: NonNullable<MacroContext["facts"]>["flows"] }) {
  if (!data || !data.days.length) return null;
  const recent = data.days.slice(-10).reverse();
  return (
    <div className="mcx-evidence" role="table" aria-label="Institutional flows">
      <div className="mcx-evidence-head mcx-evidence-head--flows" role="row">
        <span role="columnheader">Session</span>
        <span role="columnheader">FII net (₹ cr)</span>
        <span role="columnheader">DII net (₹ cr)</span>
      </div>
      {recent.map((day) => (
        <div className="mcx-evidence-row mcx-evidence-row--flows" role="row" key={day.date}>
          <span role="cell">{day.date}</span>
          <span role="cell" className={tone(day.fii_net_crore, "up_helps")}>
            {day.fii_net_crore === null ? "—" : day.fii_net_crore.toLocaleString(undefined, { maximumFractionDigits: 0 })}
          </span>
          <span role="cell" className={tone(day.dii_net_crore, "up_helps")}>
            {day.dii_net_crore === null ? "—" : day.dii_net_crore.toLocaleString(undefined, { maximumFractionDigits: 0 })}
          </span>
        </div>
      ))}
    </div>
  );
}

function EventEvidence({ events }: { events: NonNullable<MacroContext["facts"]>["events"] }) {
  if (!events.length) return null;
  return (
    <ul className="mcx-events">
      {events.slice(0, 8).map((event) => (
        <li key={`${event.date}-${event.label}`} className={event.days_away <= 2 ? "mcx-event mcx-event--near" : "mcx-event"}>
          <span className="mcx-event-when">{event.date}</span>
          <span className="mcx-event-what">{event.label}</span>
          <span className="mcx-event-away">
            {event.days_away === 0 ? "today" : `in ${event.days_away}d`}
          </span>
        </li>
      ))}
    </ul>
  );
}

/**
 * Market context — the outside world in plain English.
 *
 * Prose is the product here, not decoration on a grid: the page answers
 * "what is happening and does it let me take risk today", which is a question
 * that needs sentences. Every figure the prose can cite is still one click
 * away under each section, on the same principle as the regime brief — a
 * reader who distrusts a paragraph must be able to check it without leaving
 * the page.
 */
export function MacroContextPanel({ market }: Props) {
  const [data, setData] = useState<MacroContext | null>(null);
  const [loading, setLoading] = useState(true);
  const [refreshing, setRefreshing] = useState(false);
  const [error, setError] = useState<string | null>(null);
  const [openEvidence, setOpenEvidence] = useState<Record<string, boolean>>({});

  // Monotonic request id: a slow first load must not overwrite a newer refresh.
  const requestId = useRef(0);

  const load = useCallback(async (marketKey: MarketKey, refresh: boolean) => {
    const id = ++requestId.current;
    if (refresh) setRefreshing(true);
    else setLoading(true);
    try {
      const result = await getMacroContext(marketKey, refresh);
      if (id !== requestId.current) return;
      setData(result);
      setError(null);
    } catch (err) {
      if (id !== requestId.current) return;
      setError(err instanceof Error ? err.message : "Could not load the market context.");
    } finally {
      if (id === requestId.current) {
        setLoading(false);
        setRefreshing(false);
      }
    }
  }, []);

  useEffect(() => {
    void load(market, false);
  }, [load, market]);

  // The checklist is a discipline tool, so it resets every session rather than
  // carrying yesterday's ticks forward — a pre-market check you did yesterday
  // is not a pre-market check.
  const sessionKey = data?.as_of ?? "unknown";
  const storageKey = `macro-checklist:${market}:${sessionKey}`;
  const [ticked, setTicked] = useState<Record<string, boolean>>({});
  useEffect(() => {
    try {
      const raw = window.localStorage.getItem(storageKey);
      setTicked(raw ? (JSON.parse(raw) as Record<string, boolean>) : {});
    } catch {
      setTicked({});
    }
  }, [storageKey]);
  const toggleTick = useCallback(
    (id: string) => {
      setTicked((prev) => {
        const next = { ...prev, [id]: !prev[id] };
        try {
          window.localStorage.setItem(storageKey, JSON.stringify(next));
        } catch {
          /* private mode or blocked storage — the tick just doesn't persist */
        }
        return next;
      });
    },
    [storageKey],
  );

  const note = data?.available ? data.note ?? null : null;
  const facts = data?.available ? data.facts ?? null : null;

  const evidenceFor = useMemo(() => {
    const series = facts?.series ?? {};
    return (sectionId: string): MacroSeriesRow[] =>
      (SECTION_EVIDENCE[sectionId] ?? [])
        .map((key) => series[key])
        .filter((row): row is MacroSeriesRow => Boolean(row));
  }, [facts]);

  if (loading && !data) {
    return (
      <section className="mcx" aria-busy="true">
        <div className="skeleton" style={{ height: 22, width: "62%", marginBottom: 14 }} />
        <div className="skeleton" style={{ height: 76, marginBottom: 10 }} />
        <div className="skeleton" style={{ height: 76 }} />
      </section>
    );
  }

  if (error && !data) {
    return (
      <section className="mcx mcx--empty">
        <AlertTriangle size={16} aria-hidden />
        <p>{error}</p>
        <button type="button" onClick={() => void load(market, true)}>Try again</button>
      </section>
    );
  }

  if (!data?.available || !note) {
    return (
      <section className="mcx mcx--empty">
        <AlertTriangle size={16} aria-hidden />
        <p>{data?.reason ?? "Market context is not available yet."}</p>
        <button type="button" onClick={() => void load(market, true)}>Refresh</button>
      </section>
    );
  }

  const ticks = note.checklist.filter((item) => ticked[item.id]).length;

  return (
    <section className="mcx" aria-label="Market context">
      <header className="mcx-head">
        <div className="mcx-head-main">
          <span className={`mcx-stance mcx-stance--${note.stance}`}>{STANCE_LABEL[note.stance]}</span>
          <h3 className="mcx-headline">{note.headline}</h3>
        </div>
        <div className="mcx-head-meta">
          {note.source === "ai" ? (
            <span className="mcx-tag" title="Written by the model over numbers computed on the server">
              <Sparkles size={12} aria-hidden /> written
            </span>
          ) : (
            <span className="mcx-tag" title="Written from the computed facts without the model">computed</span>
          )}
          {data.as_of ? <span className="mcx-asof">as of {data.as_of}</span> : null}
          <button
            type="button"
            className="mcx-refresh"
            onClick={() => void load(market, true)}
            disabled={refreshing}
            aria-label="Refresh market context"
          >
            <RefreshCw size={14} className={refreshing ? "mcx-spin" : undefined} aria-hidden />
          </button>
        </div>
      </header>

      {data.stale && data.stale_reason ? (
        <p className="mcx-stale" role="status">
          <AlertTriangle size={14} aria-hidden /> {data.stale_reason}
        </p>
      ) : null}

      <div className="mcx-summary">
        {note.summary.map((paragraph, index) => (
          <p key={index}>{paragraph}</p>
        ))}
      </div>

      <div className="mcx-sections">
        {note.sections.map((section) => {
          const rows = evidenceFor(section.id);
          const hasFlows = section.id === "flows" && Boolean(facts?.flows?.days.length);
          const hasEvents = section.id === "calendar" && Boolean(facts?.events.length);
          const hasEvidence = rows.length > 0 || hasFlows || hasEvents;
          const open = Boolean(openEvidence[section.id]);
          return (
            <article className="mcx-section" key={section.id}>
              <h4>{section.title}</h4>
              {section.paragraphs.map((paragraph, index) => (
                <p key={index}>{paragraph}</p>
              ))}
              {hasEvidence ? (
                <>
                  <button
                    type="button"
                    className="mcx-evidence-toggle"
                    aria-expanded={open}
                    onClick={() =>
                      setOpenEvidence((prev) => ({ ...prev, [section.id]: !prev[section.id] }))
                    }
                  >
                    {open ? "Hide the numbers" : "Show the numbers behind this"}
                  </button>
                  {open ? (
                    <>
                      <EvidenceRows rows={rows} />
                      {hasFlows ? <FlowEvidence data={facts?.flows ?? null} /> : null}
                      {hasEvents ? <EventEvidence events={facts?.events ?? []} /> : null}
                    </>
                  ) : null}
                </>
              ) : null}
            </article>
          );
        })}
      </div>

      {note.checklist.length ? (
        <div className="mcx-checklist">
          <h4>
            Before your first order
            <span className="mcx-checklist-count">{ticks}/{note.checklist.length}</span>
          </h4>
          <ul>
            {note.checklist.map((item) => (
              <li key={item.id}>
                <button
                  type="button"
                  className={ticked[item.id] ? "mcx-tick mcx-tick--on" : "mcx-tick"}
                  aria-pressed={Boolean(ticked[item.id])}
                  onClick={() => toggleTick(item.id)}
                >
                  <span className="mcx-tick-box" aria-hidden>
                    {ticked[item.id] ? <Check size={12} /> : null}
                  </span>
                  <span className="mcx-tick-text">
                    <strong>{item.label}</strong>
                    {item.answer ? <em>{item.answer}</em> : null}
                  </span>
                </button>
              </li>
            ))}
          </ul>
        </div>
      ) : null}

      {facts?.missing_series.length ? (
        <p className="mcx-missing">
          Not available this session: {facts.missing_series.join(", ")}. Those instruments are left out of
          the read above rather than assumed flat.
        </p>
      ) : null}
    </section>
  );
}
