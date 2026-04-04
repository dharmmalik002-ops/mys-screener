import type { ChangeEvent, ReactNode } from "react";

import type { PullBackMaMode, PullBackScanRequest } from "../lib/api";
import { Panel } from "./Panel";

type PullBackScannerPanelProps = {
  filters: PullBackScanRequest;
  onFiltersChange: (filters: PullBackScanRequest) => void;
  onApply: () => void;
  onReset: () => void;
};

type PullBackBooleanField = {
  [K in keyof PullBackScanRequest]: PullBackScanRequest[K] extends boolean ? K : never;
}[keyof PullBackScanRequest];

function updateNumber(
  filters: PullBackScanRequest,
  onFiltersChange: (filters: PullBackScanRequest) => void,
  field: keyof PullBackScanRequest,
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
  filters: PullBackScanRequest,
  onFiltersChange: (filters: PullBackScanRequest) => void,
  field: PullBackBooleanField,
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
    <div className={checked ? "scanner-field pullback-field" : "scanner-field pullback-field disabled"}>
      <label className="scanner-checkbox-line">
        <input type="checkbox" checked={checked} onChange={onChange} />
        <span>{title}</span>
      </label>
      {children}
      {hint ? <small>{hint}</small> : null}
    </div>
  );
}

export function PullBackScannerPanel({
  filters,
  onFiltersChange,
  onApply,
  onReset,
}: PullBackScannerPanelProps) {
  return (
    <Panel
      title="Pull Backs"
      subtitle="Scan for momentum stocks with a 40-day run, sideways consolidation, low 3-day volume, and EMA support."
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
      <div className="scanner-section-grid pull-back-grid">
        <ToggleField
          checked={filters.enable_rs_rating}
          onChange={updateBoolean(filters, onFiltersChange, "enable_rs_rating")}
          title="Minimum RS Rating"
          hint="Default 70"
        >
          <input
            type="number"
            min="1"
            max="99"
            step="1"
            value={filters.min_rs_rating}
            onChange={updateNumber(filters, onFiltersChange, "min_rs_rating", 1)}
            disabled={!filters.enable_rs_rating}
          />
        </ToggleField>

        <ToggleField
          checked={filters.enable_first_leg_up}
          onChange={updateBoolean(filters, onFiltersChange, "enable_first_leg_up")}
          title="40D Price Up %"
          hint="Stock should be up at least 20% to 30% over the last 40 sessions"
        >
          <input
            type="number"
            min="0"
            max="200"
            step="0.5"
            value={filters.min_first_leg_up_pct}
            onChange={updateNumber(filters, onFiltersChange, "min_first_leg_up_pct", 0)}
            disabled={!filters.enable_first_leg_up}
          />
        </ToggleField>

        <ToggleField
          checked={filters.enable_consolidation_range}
          onChange={updateBoolean(filters, onFiltersChange, "enable_consolidation_range")}
          title="Consolidation Range %"
          hint="Maximum trailing sideways range"
        >
          <input
            type="number"
            min="0.1"
            max="30"
            step="0.1"
            value={filters.max_consolidation_range_pct}
            onChange={updateNumber(filters, onFiltersChange, "max_consolidation_range_pct", 0.1)}
            disabled={!filters.enable_consolidation_range}
          />
        </ToggleField>

        <ToggleField
          checked={filters.enable_consolidation_days}
          onChange={updateBoolean(filters, onFiltersChange, "enable_consolidation_days")}
          title="Consolidation Days"
          hint="Minimum days spent tightening sideways"
        >
          <input
            type="number"
            min="2"
            max="20"
            step="1"
            value={filters.min_consolidation_days}
            onChange={updateNumber(filters, onFiltersChange, "min_consolidation_days", 2)}
            disabled={!filters.enable_consolidation_days}
          />
        </ToggleField>

        <ToggleField
          checked={filters.enable_volume_contraction}
          onChange={updateBoolean(filters, onFiltersChange, "enable_volume_contraction")}
          title="3D Volume vs 20D Avg"
          hint="Each of the last 3 volumes must stay below this multiple of 20D average volume"
        >
          <input
            type="number"
            min="0.1"
            max="3"
            step="0.05"
            value={filters.max_recent_volume_vs_avg20}
            onChange={updateNumber(filters, onFiltersChange, "max_recent_volume_vs_avg20", 0.1)}
            disabled={!filters.enable_volume_contraction}
          />
        </ToggleField>

        <ToggleField
          checked={filters.enable_ma_support}
          onChange={updateBoolean(filters, onFiltersChange, "enable_ma_support")}
          title="EMA Support"
          hint="EMA20 is the default support test for this setup"
        >
          <select
            value={filters.pullback_ma}
            onChange={(event) =>
              onFiltersChange({
                ...filters,
                pullback_ma: event.target.value as PullBackMaMode,
              })
            }
            disabled={!filters.enable_ma_support}
          >
            <option value="ema20">EMA20 only</option>
            <option value="either">EMA10 or EMA20</option>
            <option value="ema10">EMA10 only</option>
          </select>
          <input
            type="number"
            min="0.1"
            max="20"
            step="0.1"
            value={filters.max_ma_distance_pct}
            onChange={updateNumber(filters, onFiltersChange, "max_ma_distance_pct", 0.1)}
            disabled={!filters.enable_ma_support}
          />
        </ToggleField>

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
          <small>Keep high if you want the full eligible universe back</small>
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
          <small>Optional floor to avoid illiquid pullback setups</small>
        </label>
      </div>
    </Panel>
  );
}
