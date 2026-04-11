import type { ChangeEvent, ReactNode } from "react";

import type { ReturnsScanRequest } from "../lib/api";
import { Panel } from "./Panel";

type ReturnsScannerPanelProps = {
  filters: ReturnsScanRequest;
  onFiltersChange: (filters: ReturnsScanRequest) => void;
  onApply: () => void;
  onReset: () => void;
};

const TIMEFRAMES = ["1D", "1W", "1M", "3M"] as const;

function updateNumber(
  filters: ReturnsScanRequest,
  onFiltersChange: (filters: ReturnsScanRequest) => void,
  field: keyof ReturnsScanRequest,
  minValue?: number,
) {
  return (event: ChangeEvent<HTMLInputElement>) => {
    const nextValue = event.target.value === "" ? null : Number(event.target.value);
    if (nextValue !== null && !Number.isFinite(nextValue)) {
      return;
    }
    onFiltersChange({
      ...filters,
      [field]: minValue && nextValue ? Math.max(minValue, nextValue) : nextValue,
    });
  };
}

function updateBoolean(
  filters: ReturnsScanRequest,
  onFiltersChange: (filters: ReturnsScanRequest) => void,
  field: keyof ReturnsScanRequest,
) {
  return (event: ChangeEvent<HTMLInputElement>) => {
    onFiltersChange({
      ...filters,
      [field]: event.target.checked,
    });
  };
}

function ToggleField({
  checked,
  onChange,
  title,
  children,
  hint,
}: {
  checked: boolean;
  onChange: (event: ChangeEvent<HTMLInputElement>) => void;
  title: string;
  children: ReactNode;
  hint?: string;
}) {
  return (
    <div className={checked ? "scanner-field returns-field" : "scanner-field returns-field disabled"}>
      <label className="scanner-checkbox-line">
        <input type="checkbox" checked={checked} onChange={onChange} />
        <span>{title}</span>
      </label>
      {children}
      {hint ? <small>{hint}</small> : null}
    </div>
  );
}

export function ReturnsScannerPanel({
  filters,
  onFiltersChange,
  onApply,
  onReset,
}: ReturnsScannerPanelProps) {
  return (
    <Panel
      title="Returns"
      subtitle="Scan for stocks with returns using optional consolidation, volume, and price movement filters."
      actions={
        <div className="custom-panel-actions">
          <button type="button" className="nav-button ghost" onClick={onReset}>
            Reset
          </button>
          <button type="button" className="nav-button primary" onClick={onApply}>
            Apply Filters
          </button>
        </div>
      }
      className="returns-panel"
    >
      <div className="scanner-section-grid returns-grid">
        {/* Time Period Selection */}
        <div className="scanner-field returns-field">
          <label className="scanner-label">Time Period</label>
          <div className="timeframe-options">
            {TIMEFRAMES.map((tf) => (
              <button
                key={tf}
                type="button"
                className={filters.timeframe === tf ? "tool-pill active" : "tool-pill"}
                onClick={() =>
                  onFiltersChange({
                    ...filters,
                    timeframe: tf,
                  })
                }
              >
                {tf === "1D" ? "1 Day" : tf === "1W" ? "1 Week" : tf === "1M" ? "1 Month" : "3 Months"}
              </button>
            ))}
          </div>
        </div>

        {/* Return Range */}
        <div className="scanner-field returns-field">
          <label className="scanner-label">Return Range (%)</label>
          <div className="return-range-inputs">
            <div className="input-group">
              <label>From</label>
              <input
                type="number"
                step="0.1"
                placeholder="Min %"
                value={filters.min_return_pct ?? ""}
                onChange={updateNumber(filters, onFiltersChange, "min_return_pct")}
              />
            </div>
            <div className="input-group">
              <label>To</label>
              <input
                type="number"
                step="0.1"
                placeholder="Max %"
                value={filters.max_return_pct ?? ""}
                onChange={updateNumber(filters, onFiltersChange, "max_return_pct")}
              />
            </div>
          </div>
        </div>

        {/* MA Checks */}
        <div className="scanner-field returns-field">
          <label className="scanner-label">Quick MA Checks</label>
          <div className="ma-checks">
            <label className="scanner-checkbox-line">
              <input
                type="checkbox"
                checked={filters.above_21_ema}
                onChange={updateBoolean(filters, onFiltersChange, "above_21_ema")}
              />
              <span>Price above 21 EMA</span>
            </label>
            <label className="scanner-checkbox-line">
              <input
                type="checkbox"
                checked={filters.above_50_ema}
                onChange={updateBoolean(filters, onFiltersChange, "above_50_ema")}
              />
              <span>Price above 50 EMA</span>
            </label>
            <label className="scanner-checkbox-line">
              <input
                type="checkbox"
                checked={filters.above_200_sma}
                onChange={updateBoolean(filters, onFiltersChange, "above_200_sma")}
              />
              <span>Price above 200 SMA</span>
            </label>
          </div>
        </div>

        {/* First Leg Up Detection */}
        <ToggleField
          checked={filters.enable_first_leg_up}
          onChange={updateBoolean(filters, onFiltersChange, "enable_first_leg_up")}
          title="Stock First Leg Up"
          hint="Minimum % move up before consolidation (40-day period)"
        >
          <input
            type="number"
            min="0"
            max="500"
            step="0.5"
            value={filters.min_first_leg_up_pct}
            onChange={updateNumber(filters, onFiltersChange, "min_first_leg_up_pct", 0)}
            disabled={!filters.enable_first_leg_up}
            placeholder="Min % (default 15%)"
          />
        </ToggleField>

        {/* Consolidation After Move */}
        <ToggleField
          checked={filters.enable_consolidation_filter}
          onChange={updateBoolean(filters, onFiltersChange, "enable_consolidation_filter")}
          title="Consolidation After Move"
          hint="Stock consolidating with controls for drawdown and range"
        >
          <div className="toggle-field-inputs">
            <div className="input-group">
              <label>Max Drawdown %</label>
              <input
                type="number"
                min="0.1"
                max="50"
                step="0.5"
                value={filters.max_drawdown_after_leg_up}
                onChange={updateNumber(filters, onFiltersChange, "max_drawdown_after_leg_up", 0.1)}
                disabled={!filters.enable_consolidation_filter}
              />
            </div>
            <div className="input-group">
              <label>Max Range %</label>
              <input
                type="number"
                min="0.1"
                max="30"
                step="0.1"
                value={filters.max_consolidation_range_pct}
                onChange={updateNumber(filters, onFiltersChange, "max_consolidation_range_pct", 0.1)}
                disabled={!filters.enable_consolidation_filter}
              />
            </div>
            <div className="input-group">
              <label>Min Days</label>
              <input
                type="number"
                min="2"
                max="20"
                step="1"
                value={filters.min_consolidation_days}
                onChange={updateNumber(filters, onFiltersChange, "min_consolidation_days", 2)}
                disabled={!filters.enable_consolidation_filter}
              />
            </div>
          </div>
        </ToggleField>

        {/* Volume Contraction */}
        <ToggleField
          checked={filters.enable_volume_contraction}
          onChange={updateBoolean(filters, onFiltersChange, "enable_volume_contraction")}
          title="Volume Dry Up"
          hint="Recent volume vs 50-day moving average (0.85 = 85% of average)"
        >
          <input
            type="number"
            min="0.1"
            max="1.0"
            step="0.05"
            value={filters.max_volume_vs_50d_avg}
            onChange={updateNumber(filters, onFiltersChange, "max_volume_vs_50d_avg", 0.1)}
            disabled={!filters.enable_volume_contraction}
            placeholder="Max ratio (default 0.85)"
          />
        </ToggleField>

        {/* Single Day Price Move */}
        <ToggleField
          checked={filters.enable_price_move_filter}
          onChange={updateBoolean(filters, onFiltersChange, "enable_price_move_filter")}
          title="Single Day Price Move"
          hint="Filter for specific one-day move percentage range"
        >
          <div className="toggle-field-inputs">
            <div className="input-group">
              <label>Min Move %</label>
              <input
                type="number"
                min="0.1"
                max="50"
                step="0.1"
                value={filters.min_price_move_pct}
                onChange={updateNumber(filters, onFiltersChange, "min_price_move_pct", 0.1)}
                disabled={!filters.enable_price_move_filter}
              />
            </div>
            <div className="input-group">
              <label>Max Move %</label>
              <input
                type="number"
                min="0.1"
                max="100"
                step="0.1"
                value={filters.max_price_move_pct}
                onChange={updateNumber(filters, onFiltersChange, "max_price_move_pct", 0.1)}
                disabled={!filters.enable_price_move_filter}
              />
            </div>
          </div>
        </ToggleField>

        <div className="scanner-field returns-field">
          <label className="scanner-label">Min Liquidity (30D Avg Traded Value, Cr)</label>
          <input
            type="number"
            min="0"
            step="1"
            placeholder="Leave blank for any liquidity"
            value={filters.min_liquidity_crore ?? ""}
            onChange={(event) =>
              onFiltersChange({
                ...filters,
                min_liquidity_crore: event.target.value === "" ? null : Math.max(0, Number(event.target.value)),
              })
            }
          />
        </div>
      </div>
    </Panel>
  );
}
