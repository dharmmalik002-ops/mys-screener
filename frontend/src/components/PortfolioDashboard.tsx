import { useEffect, useMemo, useRef, useState } from "react";
import { ColorType, LineStyle, createChart } from "lightweight-charts";
import {
  getMfPortfolioTimeline,
  type MfPortfolioResponse,
  type MfPortfolioTimeline,
  type MfValuedPosition,
} from "../lib/api";

import "./PortfolioDashboard.css";

/**
 * The portfolio, presented.
 *
 * Design intent, since it drives most of the decisions here: one focal number
 * (what the holdings are worth), one focal chart, then density. No pie of a
 * dozen rainbow wedges — allocation uses a single accent ramp so the eye reads
 * *order* rather than hunting a legend. Colour is spent only where it carries
 * meaning: green and red for money made and lost, the accent for the current
 * selection, and grey for everything else.
 *
 * Numbers are set in tabular figures and right-aligned so columns of rupees
 * line up on the decimal, which is most of what separates a statement that
 * looks considered from one that looks generated.
 */

const RANGE_LABELS: Record<string, string> = {
  "1m": "1M", "3m": "3M", "6m": "6M", "1y": "1Y",
  "2y": "2Y", "3y": "3Y", "5y": "5Y", "10y": "10Y", max: "All",
};

/** Single-hue ramp, darkest for the largest slice. */
const RAMP = [
  "#38bdf8", "#3aa8e0", "#3b93c8", "#3b7fb0", "#3a6b98",
  "#385880", "#354668", "#313551", "#2b263b", "#241a27",
];

const inr = (value: number | null | undefined, options: { compact?: boolean; sign?: boolean } = {}): string => {
  if (typeof value !== "number" || !Number.isFinite(value)) return "—";
  const sign = options.sign && value > 0 ? "+" : value < 0 ? "−" : "";
  const magnitude = Math.abs(value);
  if (options.compact) {
    if (magnitude >= 1e7) return `${sign}₹${(magnitude / 1e7).toFixed(2)} cr`;
    if (magnitude >= 1e5) return `${sign}₹${(magnitude / 1e5).toFixed(2)} L`;
    if (magnitude >= 1000) return `${sign}₹${(magnitude / 1000).toFixed(1)}k`;
  }
  return `${sign}₹${magnitude.toLocaleString("en-IN", { maximumFractionDigits: 0 })}`;
};

const pct = (value: number | null | undefined, digits = 2): string =>
  typeof value === "number" && Number.isFinite(value) ? `${value.toFixed(digits)}%` : "—";

const signedPct = (value: number | null | undefined, digits = 2): string =>
  typeof value === "number" && Number.isFinite(value)
    ? `${value > 0 ? "+" : value < 0 ? "−" : ""}${Math.abs(value).toFixed(digits)}%`
    : "—";

const units = (value: number | null | undefined): string =>
  typeof value === "number" && Number.isFinite(value)
    ? value.toLocaleString("en-IN", { minimumFractionDigits: 3, maximumFractionDigits: 3 })
    : "—";

const toneOf = (value: number | null | undefined): string =>
  typeof value === "number" && Number.isFinite(value) ? (value < 0 ? "is-down" : value > 0 ? "is-up" : "") : "";

/* ------------------------------------------------------------------ donut */

function AllocationDonut({
  slices,
  active,
  onHover,
}: {
  slices: { label: string; value: number; pct: number }[];
  active: number | null;
  onHover: (index: number | null) => void;
}) {
  const radius = 62;
  const stroke = 15;
  const circumference = 2 * Math.PI * radius;
  let offset = 0;

  return (
    <svg className="pfd-donut" viewBox="0 0 160 160" role="img" aria-label="Allocation by category">
      <g transform="translate(80,80) rotate(-90)">
        {slices.map((slice, index) => {
          const length = (slice.pct / 100) * circumference;
          const dash = `${Math.max(0, length - 1.5)} ${circumference - Math.max(0, length - 1.5)}`;
          const element = (
            <circle
              key={slice.label}
              r={radius}
              fill="none"
              stroke={RAMP[index % RAMP.length]}
              strokeWidth={active === index ? stroke + 4 : stroke}
              strokeDasharray={dash}
              strokeDashoffset={-offset}
              opacity={active === null || active === index ? 1 : 0.35}
              onMouseEnter={() => onHover(index)}
              onMouseLeave={() => onHover(null)}
            />
          );
          offset += length;
          return element;
        })}
      </g>
    </svg>
  );
}

/* ------------------------------------------------------------------- chart */

function ValueChart({
  timeline,
  height = 260,
}: {
  timeline: MfPortfolioTimeline;
  height?: number;
}) {
  const containerRef = useRef<HTMLDivElement | null>(null);
  const [readout, setReadout] = useState<{ date: string; value: number } | null>(null);

  useEffect(() => {
    const node = containerRef.current;
    if (!node || timeline.dates.length < 2) return;

    const styles = getComputedStyle(document.documentElement);
    const textColor = styles.getPropertyValue("--text-muted").trim() || "#64748b";

    const chart = createChart(node, {
      height,
      layout: {
        background: { type: ColorType.Solid, color: "transparent" },
        textColor,
        fontSize: 11,
        fontFamily: "'JetBrains Mono', 'SF Mono', Menlo, monospace",
      },
      // "₹21.7L" rather than "2170551" — a seven-digit axis label is the
      // fastest way to make a chart look unfinished.
      localization: { priceFormatter: (value: number) => inr(value, { compact: true }) },
      // Deliberately no vertical grid: it fights the area fill and adds noise
      // to what should read as one clean shape.
      grid: {
        vertLines: { visible: false },
        horzLines: { color: "rgba(148,163,184,0.09)" },
      },
      rightPriceScale: { borderVisible: false, scaleMargins: { top: 0.14, bottom: 0.06 } },
      timeScale: { borderVisible: false, fixLeftEdge: true, fixRightEdge: true },
      crosshair: {
        mode: 1,
        vertLine: { color: "rgba(56,189,248,0.5)", width: 1, style: LineStyle.Dotted, labelVisible: false },
        horzLine: { color: "rgba(56,189,248,0.35)", width: 1, style: LineStyle.Dotted, labelVisible: true },
      },
      handleScale: { axisPressedMouseMove: { time: true, price: false } },
      autoSize: true,
    });

    const area = chart.addAreaSeries({
      lineColor: "#38bdf8",
      lineWidth: 2,
      topColor: "rgba(56,189,248,0.28)",
      bottomColor: "rgba(56,189,248,0.01)",
      priceLineVisible: false,
      lastValueVisible: false,
      crosshairMarkerRadius: 4,
      crosshairMarkerBorderColor: "#38bdf8",
      crosshairMarkerBackgroundColor: "#0b1120",
      priceFormat: { type: "price", precision: 0, minMove: 1 },
    });
    area.setData(timeline.dates.map((date, index) => ({ time: date, value: timeline.values[index] })));

    // Cost basis as a reference line — the distance between it and the area is
    // the unrealised gain, which is the whole point of the picture.
    if (timeline.invested) {
      const cost = chart.addLineSeries({
        color: "rgba(148,163,184,0.55)",
        lineWidth: 1,
        lineStyle: LineStyle.Dashed,
        priceLineVisible: false,
        lastValueVisible: false,
        crosshairMarkerVisible: false,
      });
      cost.setData(timeline.dates.map((date) => ({ time: date, value: timeline.invested as number })));
    }

    const last = timeline.dates.length - 1;
    setReadout({ date: timeline.dates[last], value: timeline.values[last] });

    const byDate = new Map(timeline.dates.map((date, index) => [date, timeline.values[index]]));
    chart.subscribeCrosshairMove((param) => {
      const date = typeof param.time === "string" ? param.time : null;
      const value = date ? byDate.get(date) : undefined;
      setReadout(
        date && value !== undefined
          ? { date, value }
          : { date: timeline.dates[last], value: timeline.values[last] },
      );
    });

    chart.timeScale().fitContent();
    return () => chart.remove();
  }, [timeline, height]);

  if (timeline.dates.length < 2) {
    return <div className="pfd-chart-empty">Not enough overlapping NAV history to chart this range.</div>;
  }

  const first = timeline.values[0];
  const delta = readout ? readout.value - first : null;
  const deltaPct = readout && first ? (readout.value / first - 1) * 100 : null;

  return (
    <div className="pfd-chart">
      <div className="pfd-chart-readout">
        <span className="pfd-chart-value">{inr(readout?.value)}</span>
        <span className={`pfd-chart-delta ${toneOf(delta)}`}>
          {inr(delta, { sign: true, compact: true })} <em>{signedPct(deltaPct)}</em>
        </span>
        <span className="pfd-chart-date">{readout?.date}</span>
      </div>
      <div ref={containerRef} className="pfd-chart-canvas" style={{ height }} />
    </div>
  );
}

/* --------------------------------------------------------------- dashboard */

export function PortfolioDashboard({
  portfolio,
  onOpenFund,
  onEdit,
  onRemove,
  editing,
  confirmDelete,
  onConfirmDelete,
  busy,
}: {
  portfolio: MfPortfolioResponse;
  onOpenFund: (schemeCode: string) => void;
  onEdit: (schemeCode: string | null) => void;
  onRemove: (schemeCode: string) => void;
  editing: string | null;
  confirmDelete: string | null;
  onConfirmDelete: (schemeCode: string | null) => void;
  busy: boolean;
}) {
  const [range, setRange] = useState("1y");
  const [timeline, setTimeline] = useState<MfPortfolioTimeline | null>(null);
  const [loadingChart, setLoadingChart] = useState(true);
  const [activeSlice, setActiveSlice] = useState<number | null>(null);

  const totals = portfolio.totals;
  const open = useMemo(
    () => portfolio.positions.filter((p) => (p.units ?? 0) > 0)
      .sort((a, b) => (b.current_value ?? 0) - (a.current_value ?? 0)),
    [portfolio.positions],
  );
  const closed = useMemo(
    () => portfolio.positions.filter((p) => (p.units ?? 0) <= 0)
      .sort((a, b) => (b.realised_pnl ?? 0) - (a.realised_pnl ?? 0)),
    [portfolio.positions],
  );

  useEffect(() => {
    let cancelled = false;
    setLoadingChart(true);
    getMfPortfolioTimeline(range)
      .then((payload) => { if (!cancelled) setTimeline(payload); })
      .catch(() => { if (!cancelled) setTimeline(null); })
      .finally(() => { if (!cancelled) setLoadingChart(false); });
    return () => { cancelled = true; };
  }, [range]);

  const slices = useMemo(() => {
    const byCategory = portfolio.allocation?.by_sub_category ?? {};
    const total = Object.values(byCategory).reduce((sum, value) => sum + value, 0) || 1;
    return Object.entries(byCategory)
      .sort((a, b) => b[1] - a[1])
      .map(([label, value]) => ({ label, value, pct: (value / total) * 100 }));
  }, [portfolio.allocation]);

  const biggestWeight = Math.max(...open.map((p) => p.weight_pct ?? 0), 1);
  const hasImported = portfolio.positions.some((p) => p.cost_basis_only);

  return (
    <div className="pfd">
      {/* ------------------------------------------------------------ hero */}
      <section className="pfd-hero">
        <div className="pfd-hero-main">
          <span className="pfd-label">Current value</span>
          <h2 className="pfd-hero-value">{inr(totals?.current_value)}</h2>
          <div className="pfd-hero-delta">
            <span className={`pfd-chip ${toneOf(totals?.gain)}`}>
              {inr(totals?.gain, { sign: true })} <em>{signedPct(totals?.gain_pct)}</em>
            </span>
            <span className="pfd-hero-sub">
              on {inr(totals?.invested, { compact: true })} invested
              {totals?.open_position_count ? ` across ${totals.open_position_count} funds` : ""}
            </span>
          </div>
        </div>

        <div className="pfd-hero-splits">
          <div className="pfd-split">
            <span className="pfd-label">Unrealised</span>
            <strong className={toneOf(totals?.unrealised_pnl)}>
              {inr(totals?.unrealised_pnl, { sign: true, compact: true })}
            </strong>
            <small>{signedPct(totals?.unrealised_pct)} on what you still hold</small>
          </div>
          <div className="pfd-split">
            <span className="pfd-label">Realised</span>
            <strong className={toneOf(totals?.realised_pnl)}>
              {inr(totals?.realised_pnl, { sign: true, compact: true })}
            </strong>
            <small>
              {closed.length ? `banked across ${closed.length} exited fund${closed.length > 1 ? "s" : ""}` : "nothing sold yet"}
            </small>
          </div>
          <div className="pfd-split">
            <span className="pfd-label">Monthly SIP</span>
            <strong>{totals?.monthly_sip ? inr(totals.monthly_sip, { compact: true }) : "—"}</strong>
            <small>
              {totals?.active_sip_count
                ? `${totals.active_sip_count} active plan${totals.active_sip_count > 1 ? "s" : ""}`
                : "no SIP recorded"}
            </small>
          </div>
          <div className="pfd-split">
            <span className="pfd-label">XIRR</span>
            <strong className={toneOf(totals?.xirr)}>{pct(totals?.xirr)}</strong>
            <small>
              {totals?.xirr == null
                ? hasImported ? "needs purchase dates" : "needs 3+ months of history"
                : "money-weighted, annualised"}
            </small>
          </div>
        </div>
      </section>

      {portfolio.upcoming_sips?.length ? (
        <section className="pfd-panel pfd-panel-sips">
          <header className="pfd-panel-head">
            <h3>Next instalments</h3>
            <span className="pfd-muted">
              {inr(totals?.monthly_sip, { compact: true })} a month committed
            </span>
          </header>
          <ul className="pfd-sips">
            {portfolio.upcoming_sips.map((sip) => (
              <li key={`${sip.scheme_code}-${sip.date}`} onClick={() => onOpenFund(sip.scheme_code)}>
                <span className="pfd-sip-date">{sip.date}</span>
                <span className="pfd-sip-name">{sip.name ?? sip.scheme_code}</span>
                <i>{sip.frequency}</i>
                <b>{inr(sip.amount)}</b>
              </li>
            ))}
          </ul>
        </section>
      ) : null}

      {/* ----------------------------------------------------------- chart */}
      <section className="pfd-panel">
        <header className="pfd-panel-head">
          <div>
            <h3>Value of your holdings</h3>
            <p>
              Today's units valued back through time — how this basket of funds has moved together.
              Not a record of past balances, since the units were not all held throughout.
            </p>
          </div>
          <div className="pfd-ranges">
            {(timeline?.available_ranges ?? ["3m", "1y", "3y", "max"]).map((key) => (
              <button
                key={key}
                type="button"
                className={range === key ? "pfd-range is-active" : "pfd-range"}
                onClick={() => setRange(key)}
              >
                {RANGE_LABELS[key] ?? key.toUpperCase()}
              </button>
            ))}
          </div>
        </header>
        {timeline ? (
          <ValueChart timeline={timeline} />
        ) : (
          <div className="pfd-chart-empty">{loadingChart ? "Building the curve…" : "No chart available."}</div>
        )}
      </section>

      {/* ------------------------------------------------------ allocation */}
      <section className="pfd-grid">
        <div className="pfd-panel pfd-panel-alloc">
          <header className="pfd-panel-head"><h3>Where the money sits</h3></header>
          <div className="pfd-alloc">
            <div className="pfd-donut-wrap">
              <AllocationDonut slices={slices} active={activeSlice} onHover={setActiveSlice} />
              <div className="pfd-donut-centre">
                {activeSlice != null && slices[activeSlice] ? (
                  <>
                    <strong>{slices[activeSlice].pct.toFixed(1)}%</strong>
                    <small>{slices[activeSlice].label}</small>
                  </>
                ) : (
                  <>
                    <strong>{slices.length}</strong>
                    <small>categories</small>
                  </>
                )}
              </div>
            </div>
            <ul className="pfd-legend">
              {slices.map((slice, index) => (
                <li
                  key={slice.label}
                  className={activeSlice === index ? "is-active" : undefined}
                  onMouseEnter={() => setActiveSlice(index)}
                  onMouseLeave={() => setActiveSlice(null)}
                >
                  <i style={{ background: RAMP[index % RAMP.length] }} />
                  <span>{slice.label}</span>
                  <b>{slice.pct.toFixed(1)}%</b>
                  <em>{inr(slice.value, { compact: true })}</em>
                </li>
              ))}
            </ul>
          </div>
        </div>

        <div className="pfd-panel">
          <header className="pfd-panel-head"><h3>Largest holdings</h3></header>
          <ul className="pfd-bars">
            {open.slice(0, 8).map((position) => (
              <li key={position.scheme_code} onClick={() => onOpenFund(position.scheme_code)}>
                <span className="pfd-bars-name">{position.fund?.name ?? position.scheme_code}</span>
                <i className="pfd-bar">
                  <i style={{ width: `${((position.weight_pct ?? 0) / biggestWeight) * 100}%` }} />
                </i>
                <b>{pct(position.weight_pct, 1)}</b>
                <em className={toneOf(position.unrealised_pct)}>{signedPct(position.unrealised_pct, 1)}</em>
              </li>
            ))}
          </ul>
        </div>
      </section>

      {/* -------------------------------------------------------- holdings */}
      <section className="pfd-panel">
        <header className="pfd-panel-head">
          <h3>Holdings</h3>
          <span className="pfd-muted">valued {portfolio.as_of ?? "—"}</span>
        </header>
        <div className="pfd-scroll">
          <table className="pfd-table">
            <thead>
              <tr>
                <th className="is-left">Fund</th>
                <th>Units</th><th>Avg cost</th><th>NAV</th>
                <th>Invested</th><th>Current</th><th>P&amp;L</th><th>Weight</th><th />
              </tr>
            </thead>
            <tbody>
              {open.map((position) => (
                <tr key={position.scheme_code}>
                  <td className="is-left">
                    <button type="button" className="pfd-fund" onClick={() => onOpenFund(position.scheme_code)}>
                      {position.fund?.name ?? position.scheme_code}
                    </button>
                    <span className="pfd-tags">
                      {position.fund?.sub_category ? <i>{position.fund.sub_category}</i> : null}
                      {position.sip_plan?.active ? (
                        <i
                          className="pfd-tag-sip"
                          title={`₹${position.sip_plan.amount.toLocaleString("en-IN")} ${position.sip_plan.frequency}, next on ${position.sip_plan.next_date}`}
                        >
                          SIP {inr(position.sip_plan.amount, { compact: true })} {position.sip_plan.frequency}
                        </i>
                      ) : null}
                      {position.fund?.off_universe ? (
                        <i
                          className="pfd-tag-note"
                          title="A dividend-paying plan. It values correctly from its own NAV, and inherits its Growth sibling's category. Note that payouts you have already received are not reflected in NAV growth."
                        >
                          {position.fund.plan_variant ?? "IDCW"} plan
                        </i>
                      ) : null}
                    </span>
                  </td>
                  <td>{units(position.units)}</td>
                  <td>{position.avg_cost_nav?.toFixed(2) ?? "—"}</td>
                  <td>{position.latest_nav?.toFixed(2) ?? "—"}</td>
                  <td>{inr(position.invested)}</td>
                  <td className="pfd-strong">{inr(position.current_value)}</td>
                  <td className={toneOf(position.unrealised_pnl)}>
                    {inr(position.unrealised_pnl, { sign: true })}
                    <em>{signedPct(position.unrealised_pct)}</em>
                  </td>
                  <td className="pfd-weight">
                    <i><i style={{ width: `${position.weight_pct ?? 0}%` }} /></i>
                    <span>{pct(position.weight_pct, 1)}</span>
                  </td>
                  <td className="pfd-actions">
                    <button type="button" onClick={() => onEdit(editing === position.scheme_code ? null : position.scheme_code)}>
                      {editing === position.scheme_code ? "Done" : "Edit"}
                    </button>
                    {confirmDelete === position.scheme_code ? (
                      <>
                        <button type="button" className="is-danger" disabled={busy} onClick={() => onRemove(position.scheme_code)}>
                          Delete?
                        </button>
                        <button type="button" onClick={() => onConfirmDelete(null)}>Keep</button>
                      </>
                    ) : (
                      <button type="button" onClick={() => onConfirmDelete(position.scheme_code)}>Remove</button>
                    )}
                  </td>
                </tr>
              ))}
            </tbody>
          </table>
        </div>
      </section>

      {/* ---------------------------------------------------------- closed */}
      {closed.length ? (
        <section className="pfd-panel pfd-panel-closed">
          <header className="pfd-panel-head">
            <div>
              <h3>Exited</h3>
              <p>Funds fully sold. Realised figures are the broker's own, not re-derived here.</p>
            </div>
            <span className={`pfd-chip ${toneOf(totals?.realised_pnl)}`}>
              {inr(totals?.realised_pnl, { sign: true })} banked
            </span>
          </header>
          <div className="pfd-scroll">
            <table className="pfd-table">
              <thead>
                <tr>
                  <th className="is-left">Fund</th><th>Cost of units sold</th>
                  <th>Proceeds</th><th>Realised P&amp;L</th><th /><th />
                </tr>
              </thead>
              <tbody>
                {closed.map((position) => (
                  <tr key={position.scheme_code}>
                    <td className="is-left">
                      <button type="button" className="pfd-fund" onClick={() => onOpenFund(position.scheme_code)}>
                        {position.fund?.name ?? position.scheme_code}
                      </button>
                    </td>
                    <td>{inr(position.cost_of_units_sold)}</td>
                    <td>{inr(position.realised_proceeds)}</td>
                    <td className={toneOf(position.realised_pnl)}>
                      {inr(position.realised_pnl, { sign: true })}
                      <em>
                        {position.cost_of_units_sold
                          ? signedPct(((position.realised_pnl ?? 0) / position.cost_of_units_sold) * 100)
                          : ""}
                      </em>
                    </td>
                    <td />
                    <td className="pfd-actions">
                      {confirmDelete === position.scheme_code ? (
                        <>
                          <button type="button" className="is-danger" disabled={busy} onClick={() => onRemove(position.scheme_code)}>
                            Delete?
                          </button>
                          <button type="button" onClick={() => onConfirmDelete(null)}>Keep</button>
                        </>
                      ) : (
                        <button type="button" onClick={() => onConfirmDelete(position.scheme_code)}>Remove</button>
                      )}
                    </td>
                  </tr>
                ))}
              </tbody>
            </table>
          </div>
        </section>
      ) : null}

      {hasImported ? (
        <p className="pfd-footnote">
          Positions imported from a statement carry exact units and cost but no purchase dates, so
          XIRR is left blank rather than guessed. Add the actual SIP dates for a fund with{" "}
          <b>Edit</b> and it will start reporting one.
        </p>
      ) : null}
    </div>
  );
}
