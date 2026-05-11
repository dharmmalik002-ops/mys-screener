import { Panel } from "./Panel";

export type PositiveEarningsFilters = {
  // All thresholds optional so the user can dial gates back to "off".
  // null reverts to the backend default (the IBD-style spec).
  minCloseInRangePct: number | null;
  minNextDayGapPct: number | null;
  minDayRvol: number | null;
  minReturn5dPct: number | null;
  lookbackDays: number | null;
};

export const DEFAULT_POSITIVE_EARNINGS_FILTERS: PositiveEarningsFilters = {
  minCloseInRangePct: 0.75,
  minNextDayGapPct: 1.0,
  minDayRvol: 2.0,
  minReturn5dPct: 10.0,
  lookbackDays: 60,
};

type PositiveEarningsScannerPanelProps = {
  filters: PositiveEarningsFilters;
  onFiltersChange: (filters: PositiveEarningsFilters) => void;
  onApply: () => void;
  onReset: () => void;
};

function parseNumber(value: string): number | null {
  if (value === "") {
    return null;
  }
  const parsed = Number(value);
  return Number.isFinite(parsed) ? parsed : null;
}

export function PositiveEarningsScannerPanel({
  filters,
  onFiltersChange,
  onApply,
  onReset,
}: PositiveEarningsScannerPanelProps) {
  const update = <K extends keyof PositiveEarningsFilters>(key: K, value: PositiveEarningsFilters[K]) => {
    onFiltersChange({ ...filters, [key]: value });
  };

  return (
    <Panel
      title="Positive Earnings"
      subtitle="Stocks with a confirmed strong reaction to the latest quarterly result. Loosen any gate to widen the result set."
      actions={
        <div className="custom-panel-actions">
          <button type="button" className="nav-button ghost" onClick={onReset}>
            Reset
          </button>
          <button type="button" className="nav-button primary" onClick={onApply}>
            Apply Filter
          </button>
        </div>
      }
      className="gap-up-panel"
    >
      <div className="scanner-section-grid near-pivot-grid">
        <label className="scanner-field">
          <span>Min close in candle range (0 – 1)</span>
          <input
            type="number"
            min="0"
            max="1"
            step="0.05"
            value={filters.minCloseInRangePct ?? ""}
            onChange={(event) => update("minCloseInRangePct", parseNumber(event.target.value))}
          />
          <small>0.75 = close in top 25% of the day's range. Lower to relax.</small>
        </label>

        <label className="scanner-field">
          <span>Min next-day gap-up (%)</span>
          <input
            type="number"
            step="0.1"
            value={filters.minNextDayGapPct ?? ""}
            onChange={(event) => update("minNextDayGapPct", parseNumber(event.target.value))}
          />
          <small>Open the day after result vs. earnings-day close.</small>
        </label>

        <label className="scanner-field">
          <span>Min earnings-day volume (× 50-day avg)</span>
          <input
            type="number"
            min="0"
            step="0.1"
            value={filters.minDayRvol ?? ""}
            onChange={(event) => update("minDayRvol", parseNumber(event.target.value))}
          />
          <small>2.0 means the result-day volume was at least 2× the prior 50-day average.</small>
        </label>

        <label className="scanner-field">
          <span>Min 5-session return after earnings (%)</span>
          <input
            type="number"
            step="0.5"
            value={filters.minReturn5dPct ?? ""}
            onChange={(event) => update("minReturn5dPct", parseNumber(event.target.value))}
          />
          <small>Close 5 sessions later vs. earnings-day close.</small>
        </label>

        <label className="scanner-field">
          <span>Lookback (days)</span>
          <input
            type="number"
            min="1"
            max="365"
            step="1"
            value={filters.lookbackDays ?? ""}
            onChange={(event) => update("lookbackDays", parseNumber(event.target.value))}
          />
          <small>Only show stocks whose result was announced within this window.</small>
        </label>
      </div>
    </Panel>
  );
}
