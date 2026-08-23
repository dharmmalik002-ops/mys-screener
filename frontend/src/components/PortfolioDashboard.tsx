import { useEffect, useMemo, useRef, useState } from "react";
import { ColorType, LineStyle, createChart } from "lightweight-charts";
import {
  getMfConcentration,
  getMfPortfolioTimeline,
  type MfPortfolioResponse,
  type MfPortfolioTimeline,
  type MfConcentration,
  type MfValuedPosition,
} from "../lib/api";
import { Donut, DivergingBars, StackedBar, WeightBars } from "./PortfolioCharts";

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

const RAMP_TOKENS = [
  "--pfd-ramp-1", "--pfd-ramp-2", "--pfd-ramp-3", "--pfd-ramp-4",
  "--pfd-ramp-5", "--pfd-ramp-6", "--pfd-ramp-7", "--pfd-ramp-8",
];

const PALETTE_TOKENS = {
  line: "--pfd-line",
  lineSoft: "--pfd-line-soft",
  lineFaint: "--pfd-line-faint",
  costLine: "--pfd-cost-line",
  grid: "--pfd-grid",
  crosshair: "--pfd-crosshair",
  chartBg: "--pfd-chart-bg",
  up: "--pfd-up",
  down: "--pfd-down",
  text: "--text-muted",
} as const;

type Palette = Record<keyof typeof PALETTE_TOKENS, string> & { ramp: string[] };

function readPalette(node: Element | null): Palette {
  const styles = getComputedStyle(node ?? document.documentElement);
  const read = (token: string, fallback: string) =>
    styles.getPropertyValue(token).trim() || fallback;
  return {
    line: read(PALETTE_TOKENS.line, "#38bdf8"),
    lineSoft: read(PALETTE_TOKENS.lineSoft, "rgba(56,189,248,0.28)"),
    lineFaint: read(PALETTE_TOKENS.lineFaint, "rgba(56,189,248,0.01)"),
    costLine: read(PALETTE_TOKENS.costLine, "rgba(148,163,184,0.55)"),
    grid: read(PALETTE_TOKENS.grid, "rgba(148,163,184,0.09)"),
    crosshair: read(PALETTE_TOKENS.crosshair, "rgba(56,189,248,0.5)"),
    chartBg: read(PALETTE_TOKENS.chartBg, "#0b1120"),
    up: read(PALETTE_TOKENS.up, "#34d399"),
    down: read(PALETTE_TOKENS.down, "#fb7185"),
    text: read(PALETTE_TOKENS.text, "#64748b"),
    ramp: RAMP_TOKENS.map((token, index) => read(token, `hsl(${200 + index * 12} 80% 55%)`)),
  };
}

/**
 * Live palette, re-read when the theme flips.
 *
 * Canvas-drawn charts cannot inherit CSS variables the way the DOM does, so
 * their colours have to be read out and re-applied. Watching `data-theme` on
 * the root is what makes the charts actually change with the toggle instead of
 * staying dark-themed on a white page.
 */
function useThemePalette(ref: React.RefObject<HTMLElement | null>): Palette {
  const [palette, setPalette] = useState<Palette>(() => readPalette(null));

  useEffect(() => {
    const refresh = () => setPalette(readPalette(ref.current));
    refresh();
    const observer = new MutationObserver(refresh);
    observer.observe(document.documentElement, { attributes: true, attributeFilter: ["data-theme", "class"] });
    const media = window.matchMedia("(prefers-color-scheme: dark)");
    media.addEventListener("change", refresh);
    return () => { observer.disconnect(); media.removeEventListener("change", refresh); };
  }, [ref]);

  return palette;
}

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
  ramp,
  size = 160,
}: {
  slices: { label: string; value: number; pct: number }[];
  active: number | null;
  onHover: (index: number | null) => void;
  ramp: string[];
  size?: number;
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
              stroke={ramp[index % ramp.length]}
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
  palette,
  height = 260,
}: {
  timeline: MfPortfolioTimeline;
  palette: Palette;
  height?: number;
}) {
  const containerRef = useRef<HTMLDivElement | null>(null);
  const [readout, setReadout] = useState<{ date: string; value: number } | null>(null);

  useEffect(() => {
    const node = containerRef.current;
    if (!node || timeline.dates.length < 2) return;

    const textColor = palette.text;

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
        horzLines: { color: palette.grid },
      },
      rightPriceScale: { borderVisible: false, scaleMargins: { top: 0.14, bottom: 0.06 } },
      timeScale: { borderVisible: false, fixLeftEdge: true, fixRightEdge: true },
      crosshair: {
        mode: 1,
        vertLine: { color: palette.crosshair, width: 1, style: LineStyle.Dotted, labelVisible: false },
        horzLine: { color: palette.crosshair, width: 1, style: LineStyle.Dotted, labelVisible: true },
      },
      handleScale: { axisPressedMouseMove: { time: true, price: false } },
      autoSize: true,
    });

    const area = chart.addAreaSeries({
      lineColor: palette.line,
      lineWidth: 2,
      topColor: palette.lineSoft,
      bottomColor: palette.lineFaint,
      priceLineVisible: false,
      lastValueVisible: false,
      crosshairMarkerRadius: 4,
      crosshairMarkerBorderColor: palette.line,
      crosshairMarkerBackgroundColor: palette.chartBg,
      priceFormat: { type: "price", precision: 0, minMove: 1 },
    });
    area.setData(timeline.dates.map((date, index) => ({ time: date, value: timeline.values[index] })));

    // Cost basis as a reference line — the distance between it and the area is
    // the unrealised gain, which is the whole point of the picture.
    if (timeline.invested) {
      const cost = chart.addLineSeries({
        color: palette.costLine,
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
  }, [timeline, height, palette]);

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

type SortKey =
  | "name" | "sub_category" | "units" | "avg_cost_nav" | "latest_nav"
  | "invested" | "current_value" | "unrealised_pnl" | "unrealised_pct"
  | "weight_pct" | "xirr";

const SORT_COLUMNS: { key: SortKey; label: string; left?: boolean }[] = [
  { key: "name", label: "Fund", left: true },
  { key: "units", label: "Units" },
  { key: "avg_cost_nav", label: "Avg cost" },
  { key: "latest_nav", label: "NAV" },
  { key: "invested", label: "Invested" },
  { key: "current_value", label: "Current" },
  { key: "unrealised_pnl", label: "P&L" },
  { key: "unrealised_pct", label: "P&L %" },
  { key: "weight_pct", label: "Weight" },
];

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
  const rootRef = useRef<HTMLDivElement | null>(null);
  const palette = useThemePalette(rootRef);
  const [range, setRange] = useState("1y");
  const [timeline, setTimeline] = useState<MfPortfolioTimeline | null>(null);
  const [loadingChart, setLoadingChart] = useState(true);
  const [activeSlice, setActiveSlice] = useState<number | null>(null);
  const [concentration, setConcentration] = useState<MfConcentration | null>(null);
  const [sortKey, setSortKey] = useState<SortKey>("current_value");
  const [sortAsc, setSortAsc] = useState(false);

  const totals = portfolio.totals;
  const open = useMemo(() => {
    const rows = portfolio.positions.filter((p) => (p.units ?? 0) > 0);
    const pick = (row: MfValuedPosition): number | string => {
      switch (sortKey) {
        case "name": return (row.fund?.name ?? row.scheme_code).toLowerCase();
        case "sub_category": return (row.fund?.sub_category ?? "").toLowerCase();
        case "units": return row.units ?? 0;
        case "avg_cost_nav": return row.avg_cost_nav ?? 0;
        case "latest_nav": return row.latest_nav ?? 0;
        case "invested": return row.invested ?? 0;
        case "unrealised_pnl": return row.unrealised_pnl ?? 0;
        case "unrealised_pct": return row.unrealised_pct ?? 0;
        case "weight_pct": return row.weight_pct ?? 0;
        case "xirr": return row.xirr ?? -999;
        default: return row.current_value ?? 0;
      }
    };
    return [...rows].sort((a, b) => {
      const left = pick(a);
      const right = pick(b);
      const order = typeof left === "string" && typeof right === "string"
        ? left.localeCompare(right)
        : (left as number) - (right as number);
      return sortAsc ? order : -order;
    });
  }, [portfolio.positions, sortKey, sortAsc]);

  const sortBy = (key: SortKey) => {
    if (key === sortKey) { setSortAsc((value) => !value); return; }
    setSortKey(key);
    // Text sorts read better A-Z; numbers read better largest-first.
    setSortAsc(key === "name" || key === "sub_category");
  };
  const closed = useMemo(
    () => portfolio.positions.filter((p) => (p.units ?? 0) <= 0)
      .sort((a, b) => (b.realised_pnl ?? 0) - (a.realised_pnl ?? 0)),
    [portfolio.positions],
  );

  useEffect(() => {
    let cancelled = false;
    getMfConcentration()
      .then((payload) => { if (!cancelled) setConcentration(payload); })
      .catch(() => { if (!cancelled) setConcentration(null); });
    return () => { cancelled = true; };
  }, [portfolio.as_of, portfolio.positions.length]);

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

  const totalValue = totals?.current_value || 1;

  const sectorSlices = useMemo(() => {
    // Sector mix of the look-through, which is the real sector exposure —
    // a fund's own label says nothing about what it actually owns.
    const totals: Record<string, number> = {};
    for (const row of portfolio.allocation?.look_through_top ?? []) {
      const sector = row.sector ?? "Unclassified";
      totals[sector] = (totals[sector] ?? 0) + row.value;
    }
    const sum = Object.values(totals).reduce((a, b) => a + b, 0) || 1;
    return Object.entries(totals)
      .sort((a, b) => b[1] - a[1])
      .slice(0, 8)
      .map(([label, value]) => ({ label, value, pct: (value / sum) * 100 }));
  }, [portfolio.allocation]);

  const capSlices = useMemo(() => {
    const buckets: Record<string, number> = {};
    for (const row of portfolio.allocation?.look_through_top ?? []) {
      const key = row.cap_class ? `${row.cap_class[0].toUpperCase()}${row.cap_class.slice(1)} cap` : "Unclassified";
      buckets[key] = (buckets[key] ?? 0) + row.value;
    }
    const order = ["Large cap", "Mid cap", "Small cap", "Unclassified"];
    const sum = Object.values(buckets).reduce((a, b) => a + b, 0) || 1;
    return order.filter((key) => buckets[key])
      .map((label) => ({ label, value: buckets[label], pct: (buckets[label] / sum) * 100 }));
  }, [portfolio.allocation]);

  const amcSlices = useMemo(() => {
    const byAmc = portfolio.allocation?.by_amc ?? {};
    const sum = Object.values(byAmc).reduce((a, b) => a + b, 0) || 1;
    return Object.entries(byAmc).sort((a, b) => b[1] - a[1])
      .map(([label, value]) => ({ label, value, pct: (value / sum) * 100 }));
  }, [portfolio.allocation]);

  const pnlRows = useMemo(
    () => open.map((position) => ({
      label: position.fund?.name ?? position.scheme_code,
      value: position.unrealised_pnl ?? 0,
      pct: position.unrealised_pct ?? null,
    })).sort((a, b) => b.value - a.value),
    [open],
  );

  const stockRows = useMemo(
    () => (portfolio.allocation?.look_through_top ?? []).slice(0, 10).map((row) => ({
      label: row.name,
      value: row.weight_pct ?? 0,
      meta: row.fund_count > 1 ? `${row.fund_count} funds` : row.symbol ?? "",
    })),
    [portfolio.allocation],
  );

  const biggestWeight = Math.max(...open.map((p) => p.weight_pct ?? 0), 1);
  const hasImported = portfolio.positions.some((p) => p.cost_basis_only);

  return (
    <div className="pfd" ref={rootRef}>
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
          <ValueChart timeline={timeline} palette={palette} />
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
              <AllocationDonut slices={slices} active={activeSlice} onHover={setActiveSlice} ramp={palette.ramp} />
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
                  <i style={{ background: palette.ramp[index % palette.ramp.length] }} />
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

      {/* ---------------------------------------------------------- charts */}
      <section className="pfd-charts">
        <Donut
          slices={sectorSlices}
          title="Sector exposure"
          subtitle="Your funds' holdings collapsed into sectors — what you actually own, not what the fund labels say."
        />
        <Donut
          slices={amcSlices}
          title="By fund house"
          subtitle="Concentration with a single AMC is a risk no factsheet mentions."
        />
        <StackedBar
          slices={capSlices}
          title="Market cap mix"
          subtitle="SEBI classification — top 100 large, next 150 mid, rest small."
          note="Derived from this app's own market-cap data for the stocks your funds disclose."
        />
        <DivergingBars
          rows={pnlRows}
          title="Unrealised P&L by fund"
          subtitle="What each holding has actually made or lost, at today's NAV."
          onPick={(label) => {
            const match = open.find((position) => (position.fund?.name ?? position.scheme_code) === label);
            if (match) onOpenFund(match.scheme_code);
          }}
        />
        <WeightBars
          rows={stockRows}
          title="Largest stock exposures"
          subtitle="Across every fund combined. A name appearing in several funds is one position, not several."
        />
      </section>

      {/* -------------------------------------------------- concentration */}
      {concentration?.summary?.length ? (
        <section className="pfd-panel">
          <header className="pfd-panel-head">
            <div>
              <h3>Concentration check</h3>
              <p>
                How much of each fund sits in its largest holdings, and where the same stock reaches
                you through more than one fund. Measured from the latest disclosed portfolios —
                these are facts about weights, not a suggestion to change anything.
              </p>
            </div>
            <span className={concentration.concentrated_count ? "pfd-chip is-down" : "pfd-chip is-up"}>
              {concentration.concentrated_count} of {concentration.funds.length} flagged
            </span>
          </header>

          <ul className="pfd-findings">
            {concentration.summary.map((line) => <li key={line}>{line}</li>)}
          </ul>

          <div className="pfd-scroll">
            <table className="pfd-table">
              <thead>
                <tr>
                  <th className="is-left">Fund</th><th>Top 5</th><th>Top 10</th>
                  <th className="is-left">Largest holding</th><th>Weight</th>
                  <th>Stocks</th><th>Disclosed</th>
                </tr>
              </thead>
              <tbody>
                {concentration.funds.map((fund) => (
                  <tr key={fund.scheme_code} className={fund.concentrated ? "is-flagged" : undefined}>
                    <td className="is-left">
                      <button type="button" className="pfd-fund" onClick={() => onOpenFund(fund.scheme_code)}>
                        {fund.name}
                      </button>
                      <span className="pfd-tags"><i>{fund.sub_category}</i></span>
                    </td>
                    <td className={(fund.top5_pct ?? 0) > (concentration.thresholds.top5_pct ?? 30) ? "is-down pfd-strong" : ""}>
                      {pct(fund.top5_pct, 1)}
                    </td>
                    <td className={(fund.top10_pct ?? 0) > (concentration.thresholds.top10_pct ?? 50) ? "is-down" : ""}>
                      {pct(fund.top10_pct, 1)}
                    </td>
                    <td className="is-left pfd-muted">{fund.largest_name ?? "—"}</td>
                    <td className={(fund.largest_pct ?? 0) > (concentration.thresholds.single_stock_pct ?? 8) ? "is-down" : ""}>
                      {pct(fund.largest_pct, 1)}
                    </td>
                    <td className="pfd-muted">{fund.holdings_count ?? "—"}</td>
                    <td className="pfd-muted">{fund.portfolio_date ?? "—"}</td>
                  </tr>
                ))}
              </tbody>
            </table>
          </div>
        </section>
      ) : null}

      {/* -------------------------------------------------------- holdings */}
      <section className="pfd-panel">
        <header className="pfd-panel-head">
          <div>
            <h3>Holdings</h3>
            <p>Click any column to sort. Everything revalues from the latest NAV each time the page loads.</p>
          </div>
          <span className="pfd-muted">valued {portfolio.as_of ?? "—"}</span>
        </header>
        <div className="pfd-scroll">
          <table className="pfd-table">
            <thead>
              <tr>
                {SORT_COLUMNS.map((column) => (
                  <th
                    key={column.key}
                    className={`is-sortable${column.left ? " is-left" : ""}${sortKey === column.key ? " is-sorted" : ""}`}
                    onClick={() => sortBy(column.key)}
                    title={`Sort by ${column.label}`}
                  >
                    {column.label}
                    <i>{sortKey === column.key ? (sortAsc ? "▲" : "▼") : ""}</i>
                  </th>
                ))}
                <th />
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
                  </td>
                  <td className={toneOf(position.unrealised_pct)}>{signedPct(position.unrealised_pct)}</td>
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
