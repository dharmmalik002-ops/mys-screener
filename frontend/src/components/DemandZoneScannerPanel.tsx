import type { ChangeEvent } from "react";

import type { DemandZoneScanRequest } from "../lib/api";
import { Panel } from "./Panel";

type DemandZoneScannerPanelProps = {
  filters: DemandZoneScanRequest;
  onFiltersChange: (filters: DemandZoneScanRequest) => void;
  onApply: () => void;
  onReset: () => void;
};

function updateNumber(
  filters: DemandZoneScanRequest,
  onFiltersChange: (filters: DemandZoneScanRequest) => void,
  field: keyof DemandZoneScanRequest,
  minValue: number,
  maxValue?: number,
) {
  return (event: ChangeEvent<HTMLInputElement>) => {
    const value = Number(event.target.value);
    if (!Number.isFinite(value)) {
      return;
    }
    const clamped = Math.max(minValue, maxValue === undefined ? value : Math.min(maxValue, value));
    onFiltersChange({ ...filters, [field]: clamped });
  };
}

export function DemandZoneScannerPanel({
  filters,
  onFiltersChange,
  onApply,
  onReset,
}: DemandZoneScannerPanelProps) {
  return (
    <Panel
      title="Demand Zone Scanner"
      subtitle="Stage 2 stocks trading inside or within 3% above a strong weekly rally-base-rally demand zone."
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
      className="demand-zone-panel"
    >
      <div className="scanner-section-grid near-pivot-grid">
        <label className="scanner-field">
          <span>Near Zone %</span>
          <input
            type="number"
            min="0"
            max="20"
            step="0.1"
            value={filters.max_distance_above_zone_pct}
            onChange={updateNumber(filters, onFiltersChange, "max_distance_above_zone_pct", 0, 20)}
          />
          <small>Inside the zone or this far above the zone high</small>
        </label>

        <label className="scanner-field">
          <span>Minimum RS Rating</span>
          <input
            type="number"
            min="1"
            max="99"
            step="1"
            value={filters.min_rs_rating}
            onChange={updateNumber(filters, onFiltersChange, "min_rs_rating", 1, 99)}
          />
          <small>Default 70</small>
        </label>

        <label className="scanner-field">
          <span>Min Liquidity (30D Avg Traded Value, Cr)</span>
          <input
            type="number"
            min="0"
            step="1"
            value={filters.min_liquidity_crore}
            onChange={updateNumber(filters, onFiltersChange, "min_liquidity_crore", 0)}
          />
          <small>Default 5 Cr</small>
        </label>

        <label className="scanner-field">
          <span>Min Departure %</span>
          <input
            type="number"
            min="1"
            max="100"
            step="0.5"
            value={filters.min_departure_pct}
            onChange={updateNumber(filters, onFiltersChange, "min_departure_pct", 1, 100)}
          />
          <small>Weekly move away from the base</small>
        </label>

        <label className="scanner-field">
          <span>Base Min Weeks</span>
          <input
            type="number"
            min="1"
            max="12"
            step="1"
            value={filters.base_min_weeks}
            onChange={updateNumber(filters, onFiltersChange, "base_min_weeks", 1, 12)}
          />
          <small>Default 2 weeks</small>
        </label>

        <label className="scanner-field">
          <span>Base Max Weeks</span>
          <input
            type="number"
            min="1"
            max="20"
            step="1"
            value={filters.base_max_weeks}
            onChange={updateNumber(filters, onFiltersChange, "base_max_weeks", 1, 20)}
          />
          <small>Default 6 weeks</small>
        </label>

        <label className="scanner-field">
          <span>Max Base Range %</span>
          <input
            type="number"
            min="1"
            max="50"
            step="0.5"
            value={filters.max_base_range_pct}
            onChange={updateNumber(filters, onFiltersChange, "max_base_range_pct", 1, 50)}
          />
          <small>Tighter bases rank better</small>
        </label>

        <label className="scanner-field">
          <span>Max Zone Age (Weeks)</span>
          <input
            type="number"
            min="1"
            max="260"
            step="1"
            value={filters.max_zone_age_weeks}
            onChange={updateNumber(filters, onFiltersChange, "max_zone_age_weeks", 1, 260)}
          />
          <small>Default 52 weeks</small>
        </label>

        <label className="scanner-field">
          <span>Result Limit</span>
          <input
            type="number"
            min="1"
            max="5000"
            step="1"
            value={filters.limit}
            onChange={updateNumber(filters, onFiltersChange, "limit", 1, 5000)}
          />
          <small>Scan the full eligible universe if needed</small>
        </label>
      </div>
    </Panel>
  );
}
