import type { ChangeEvent } from "react";

import type { ConsolidatingScanRequest } from "../lib/api";
import { Panel } from "./Panel";

type ConsolidatingScannerPanelProps = {
  filters: ConsolidatingScanRequest;
  onFiltersChange: (filters: ConsolidatingScanRequest) => void;
  onApply: () => void;
  onReset: () => void;
};

type ConsolidatingBooleanField = {
  [K in keyof ConsolidatingScanRequest]: ConsolidatingScanRequest[K] extends boolean ? K : never;
}[keyof ConsolidatingScanRequest];

function updateNumber(
  filters: ConsolidatingScanRequest,
  onFiltersChange: (filters: ConsolidatingScanRequest) => void,
  field: keyof ConsolidatingScanRequest,
  minValue?: number,
) {
  return (event: ChangeEvent<HTMLInputElement>) => {
    const nextValue = Number(event.target.value);
    if (!Number.isFinite(nextValue)) {
      return;
    }

    onFiltersChange({
      ...filters,
      [field]: Math.max(minValue ?? nextValue, nextValue),
    });
  };
}

function updateBoolean(
  filters: ConsolidatingScanRequest,
  onFiltersChange: (filters: ConsolidatingScanRequest) => void,
  field: ConsolidatingBooleanField,
) {
  return (event: ChangeEvent<HTMLInputElement>) => {
    onFiltersChange({
      ...filters,
      [field]: event.target.checked,
    });
  };
}

export function ConsolidatingScannerPanel({
  filters,
  onFiltersChange,
  onApply,
  onReset,
}: ConsolidatingScannerPanelProps) {
  return (
    <Panel
      title="Consolidating"
      subtitle="Select long run-up consolidations, near 3-year breakouts, or the union of both."
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
      className="consolidating-panel"
    >
      <div className="scanner-toggle-grid compact consolidating-toggle-grid">
        <label className="scanner-toggle">
          <input
            type="checkbox"
            checked={filters.enable_run_up_consolidation}
            onChange={updateBoolean(filters, onFiltersChange, "enable_run_up_consolidation")}
          />
          <span>Long Consolidation After a Run-Up</span>
        </label>

        <label className="scanner-toggle">
          <input
            type="checkbox"
            checked={filters.enable_near_multi_year_breakout}
            onChange={updateBoolean(filters, onFiltersChange, "enable_near_multi_year_breakout")}
          />
          <span>Near Multi-Year Breakout</span>
        </label>
      </div>

      <div className="scanner-section-grid consolidating-grid">
        <section className={filters.enable_run_up_consolidation ? "scanner-config-card" : "scanner-config-card disabled"}>
          <div className="scanner-config-card-header">
            <h3>Long Consolidation After a Run-Up</h3>
            <small>Uptrend intact, base not too deep, last 15 sessions tight, and recent volume drying up.</small>
          </div>
          <ul className="scanner-rule-list">
            <li>Close above 50D SMA and 200D SMA, with 50D SMA above 200D SMA.</li>
            <li>Price stays above 60% of the 52-week high, and the base either never fell more than 40% or has recovered to within 10% of the 52-week high.</li>
            <li>Last 15 sessions stay inside a 9% range and 10-day average volume remains below the 50-day average.</li>
          </ul>
        </section>

        <section className={filters.enable_near_multi_year_breakout ? "scanner-config-card" : "scanner-config-card disabled"}>
          <div className="scanner-config-card-header">
            <h3>Near Multi-Year Breakout</h3>
            <small>Names sitting within 8% of a 3-year high while trend and liquidity stay healthy.</small>
          </div>
          <ul className="scanner-rule-list">
            <li>Close sits between 92% and 100% of the 3-year high.</li>
            <li>Close remains above the 50D SMA.</li>
            <li>Current volume is at least 100,000 shares.</li>
          </ul>
        </section>

        <section className="scanner-config-card">
          <div className="scanner-config-card-header">
            <h3>Results</h3>
            <small>Keep the limit high if you want the full eligible list back.</small>
          </div>

          <label className="scanner-field">
            <span>Result Limit</span>
            <input
              type="number"
              min="1"
              max="5000"
              step="1"
              value={filters.limit}
              onChange={updateNumber(filters, onFiltersChange, "limit", 1)}
            />
          </label>

          <label className="scanner-field">
            <span>Min Liquidity (30D Avg Traded Value, Cr)</span>
            <input
              type="number"
              min="0"
              step="1"
              value={filters.min_liquidity_crore ?? ""}
              onChange={(event) =>
                onFiltersChange({
                  ...filters,
                  min_liquidity_crore: event.target.value === "" ? null : Math.max(0, Number(event.target.value)),
                })
              }
            />
          </label>
        </section>
      </div>
    </Panel>
  );
}
