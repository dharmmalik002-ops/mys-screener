import { useMemo, useState } from "react";

import "./PortfolioCharts.css";

/**
 * The supporting charts: allocation by several cuts, and P&L per fund.
 *
 * All SVG, all driven by the same `--pfd-ramp-*` tokens the dashboard uses, so
 * a theme switch recolours them without any JS. Deliberately not a chart
 * library — these are simple shapes, and inline SVG means they inherit the
 * page's type and colour rather than fighting it.
 */

export type Slice = { label: string; value: number; pct: number };

const fmt = (value: number): string => {
  if (!Number.isFinite(value)) return "—";
  const magnitude = Math.abs(value);
  const sign = value < 0 ? "−" : "";
  if (magnitude >= 1e7) return `${sign}₹${(magnitude / 1e7).toFixed(2)} cr`;
  if (magnitude >= 1e5) return `${sign}₹${(magnitude / 1e5).toFixed(2)} L`;
  if (magnitude >= 1000) return `${sign}₹${(magnitude / 1000).toFixed(1)}k`;
  return `${sign}₹${Math.round(magnitude)}`;
};

/** Donut with a hover-driven centre readout. */
export function Donut({
  slices,
  title,
  subtitle,
  size = 148,
  thickness = 14,
}: {
  slices: Slice[];
  title: string;
  subtitle?: string;
  size?: number;
  thickness?: number;
}) {
  const [active, setActive] = useState<number | null>(null);
  const radius = (size - thickness - 6) / 2;
  const circumference = 2 * Math.PI * radius;
  let offset = 0;

  if (!slices.length) {
    return (
      <div className="pfc-card">
        <h4>{title}</h4>
        <p className="pfc-empty">Nothing to show yet.</p>
      </div>
    );
  }

  return (
    <div className="pfc-card">
      <h4>{title}</h4>
      {subtitle ? <p className="pfc-sub">{subtitle}</p> : null}
      <div className="pfc-donut-row">
        <div className="pfc-donut-wrap" style={{ width: size, height: size }}>
          <svg viewBox={`0 0 ${size} ${size}`} width={size} height={size} role="img" aria-label={title}>
            <g transform={`translate(${size / 2},${size / 2}) rotate(-90)`}>
              {slices.map((slice, index) => {
                const length = (slice.pct / 100) * circumference;
                const element = (
                  <circle
                    key={slice.label}
                    r={radius}
                    fill="none"
                    className={`pfc-slice pfc-ramp-${(index % 8) + 1}`}
                    strokeWidth={active === index ? thickness + 4 : thickness}
                    strokeDasharray={`${Math.max(0, length - 1.5)} ${circumference}`}
                    strokeDashoffset={-offset}
                    opacity={active === null || active === index ? 1 : 0.3}
                    onMouseEnter={() => setActive(index)}
                    onMouseLeave={() => setActive(null)}
                  />
                );
                offset += length;
                return element;
              })}
            </g>
          </svg>
          <div className="pfc-donut-centre">
            {active != null && slices[active] ? (
              <>
                <strong>{slices[active].pct.toFixed(1)}%</strong>
                <small>{slices[active].label}</small>
              </>
            ) : (
              <>
                <strong>{slices.length}</strong>
                <small>{slices.length === 1 ? "slice" : "slices"}</small>
              </>
            )}
          </div>
        </div>
        <ul className="pfc-legend">
          {slices.slice(0, 8).map((slice, index) => (
            <li
              key={slice.label}
              className={active === index ? "is-active" : undefined}
              onMouseEnter={() => setActive(index)}
              onMouseLeave={() => setActive(null)}
            >
              <i className={`pfc-swatch pfc-ramp-${(index % 8) + 1}`} />
              <span>{slice.label}</span>
              <b>{slice.pct.toFixed(1)}%</b>
              <em>{fmt(slice.value)}</em>
            </li>
          ))}
        </ul>
      </div>
    </div>
  );
}

/** Diverging bars around a zero axis — for gains and losses side by side. */
export function DivergingBars({
  rows,
  title,
  subtitle,
  onPick,
}: {
  rows: { label: string; value: number; pct?: number | null }[];
  title: string;
  subtitle?: string;
  onPick?: (label: string) => void;
}) {
  const extent = useMemo(
    () => Math.max(...rows.map((row) => Math.abs(row.value)), 1),
    [rows],
  );
  const anyNegative = rows.some((row) => row.value < 0);

  return (
    <div className="pfc-card">
      <h4>{title}</h4>
      {subtitle ? <p className="pfc-sub">{subtitle}</p> : null}
      <ul className="pfc-diverge">
        {rows.map((row) => {
          const share = (Math.abs(row.value) / extent) * 50;
          return (
            <li
              key={row.label}
              onClick={onPick ? () => onPick(row.label) : undefined}
              className={onPick ? "is-clickable" : undefined}
            >
              <span className="pfc-diverge-label">{row.label}</span>
              <span className="pfc-diverge-track">
                {anyNegative ? <i className="pfc-axis" /> : null}
                <i
                  className={row.value < 0 ? "pfc-fill is-down" : "pfc-fill is-up"}
                  style={
                    anyNegative
                      ? row.value < 0
                        ? { right: "50%", width: `${share}%` }
                        : { left: "50%", width: `${share}%` }
                      : { left: 0, width: `${share * 2}%` }
                  }
                />
              </span>
              <b className={row.value < 0 ? "is-down" : "is-up"}>{fmt(row.value)}</b>
              {row.pct != null ? (
                <em className={row.pct < 0 ? "is-down" : "is-up"}>
                  {row.pct > 0 ? "+" : row.pct < 0 ? "−" : ""}{Math.abs(row.pct).toFixed(1)}%
                </em>
              ) : <em />}
            </li>
          );
        })}
      </ul>
    </div>
  );
}

/** Single stacked bar — good for a two- or three-way split like market cap. */
export function StackedBar({
  slices,
  title,
  subtitle,
  note,
}: {
  slices: Slice[];
  title: string;
  subtitle?: string;
  note?: string;
}) {
  if (!slices.length) {
    return (
      <div className="pfc-card">
        <h4>{title}</h4>
        <p className="pfc-empty">Nothing to show yet.</p>
      </div>
    );
  }
  return (
    <div className="pfc-card">
      <h4>{title}</h4>
      {subtitle ? <p className="pfc-sub">{subtitle}</p> : null}
      <div className="pfc-stack">
        {slices.map((slice, index) => (
          <i
            key={slice.label}
            className={`pfc-ramp-${(index % 8) + 1}`}
            style={{ width: `${slice.pct}%` }}
            title={`${slice.label}: ${slice.pct.toFixed(1)}%`}
          />
        ))}
      </div>
      <ul className="pfc-stack-legend">
        {slices.map((slice, index) => (
          <li key={slice.label}>
            <i className={`pfc-swatch pfc-ramp-${(index % 8) + 1}`} />
            <span>{slice.label}</span>
            <b>{slice.pct.toFixed(1)}%</b>
          </li>
        ))}
      </ul>
      {note ? <p className="pfc-note">{note}</p> : null}
    </div>
  );
}

/** Horizontal bars, all positive — for weights and exposures. */
export function WeightBars({
  rows,
  title,
  subtitle,
  suffix = "%",
  onPick,
}: {
  rows: { label: string; value: number; meta?: string | null }[];
  title: string;
  subtitle?: string;
  suffix?: string;
  onPick?: (label: string) => void;
}) {
  const max = Math.max(...rows.map((row) => row.value), 1);
  return (
    <div className="pfc-card">
      <h4>{title}</h4>
      {subtitle ? <p className="pfc-sub">{subtitle}</p> : null}
      <ul className="pfc-weights">
        {rows.map((row, index) => (
          <li
            key={row.label}
            onClick={onPick ? () => onPick(row.label) : undefined}
            className={onPick ? "is-clickable" : undefined}
          >
            <span className="pfc-weights-label">{row.label}</span>
            <span className="pfc-weights-track">
              <i className={`pfc-ramp-${(index % 8) + 1}`} style={{ width: `${(row.value / max) * 100}%` }} />
            </span>
            <b>{row.value.toFixed(1)}{suffix}</b>
            <em>{row.meta ?? ""}</em>
          </li>
        ))}
      </ul>
    </div>
  );
}
