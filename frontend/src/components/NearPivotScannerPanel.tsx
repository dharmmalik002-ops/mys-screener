import type { ChangeEvent } from "react";

import type { NearPivotScanRequest } from "../lib/api";
import { Panel } from "./Panel";

type NearPivotScannerPanelProps = {
  filters: NearPivotScanRequest;
  onFiltersChange: (filters: NearPivotScanRequest) => void;
  onApply: () => void;
  onReset: () => void;
};

function updateNumber(
  filters: NearPivotScanRequest,
  onFiltersChange: (filters: NearPivotScanRequest) => void,
  field: keyof NearPivotScanRequest,
  minValue?: number,
) {
  return (event: ChangeEvent<HTMLInputElement>) => {
    const nextValue = Number(event.target.value);
    onFiltersChange({
      ...filters,
      [field]: Number.isFinite(nextValue) ? Math.max(minValue ?? nextValue, nextValue) : filters[field],
    });
  };
}

export function NearPivotScannerPanel({
  filters,
  onFiltersChange,
  onApply,
  onReset,
}: NearPivotScannerPanelProps) {
  return (
    <Panel
      title="Near Pivot"
      subtitle="High-RS stocks within 20% of their 52-week high and holding a tight recent consolidation."
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
      className="gap-up-panel"
    >
      <div className="scanner-section-grid near-pivot-grid">
        <label className="scanner-field">
          <span>Minimum RS Rating</span>
          <input
            type="number"
            min="1"
            max="99"
            step="1"
            value={filters.min_rs_rating}
            onChange={updateNumber(filters, onFiltersChange, "min_rs_rating", 1)}
          />
          <small>Default 70</small>
        </label>

        <label className="scanner-field">
          <span>Consolidation Range %</span>
          <input
            type="number"
            min="0.1"
            max="25"
            step="0.1"
            value={filters.max_consolidation_range_pct}
            onChange={updateNumber(filters, onFiltersChange, "max_consolidation_range_pct", 0.1)}
          />
          <small>Maximum range across the consolidation window</small>
        </label>

        <label className="scanner-field">
          <span>Consolidation Days</span>
          <input
            type="number"
            min="2"
            max="20"
            step="1"
            value={filters.min_consolidation_days}
            onChange={updateNumber(filters, onFiltersChange, "min_consolidation_days", 2)}
          />
          <small>Minimum trailing sessions that stay tight</small>
        </label>

        <label className="scanner-field">
          <span>52W High Distance %</span>
          <input
            type="number"
            min="0"
            max="50"
            step="0.1"
            value={filters.max_pct_from_52w_high}
            onChange={updateNumber(filters, onFiltersChange, "max_pct_from_52w_high", 0)}
          />
          <small>Default 20%</small>
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
          <small>Leave blank to scan the full eligible universe</small>
        </label>

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
          <small>Scan the whole eligible universe if needed</small>
        </label>
      </div>
    </Panel>
  );
}
