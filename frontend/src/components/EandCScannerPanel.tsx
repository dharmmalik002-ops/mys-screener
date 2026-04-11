import { Panel } from "./Panel";

export type EandCViewMode = "contraction" | "expansion" | "both";

export type EandCSettings = {
  min_price: number;
  contraction_min_avg_volume_50d: number;
  contraction_min_day_volume: number;
  contraction_require_above_ema50: boolean;
  contraction_max_price_to_sma50_ratio: number;
  contraction_max_today_change_abs_pct: number;
  contraction_max_prev_day_change_abs_pct: number;
  contraction_max_two_days_ago_change_abs_pct: number;
  contraction_require_prior_run_up: boolean;
  contraction_min_return_5d: number;
  contraction_min_return_20d: number;
  contraction_min_return_60d: number;
  expansion_min_change_pct: number;
  expansion_min_avg_volume_50d: number;
  expansion_min_day_volume: number;
  expansion_min_volume_multiple: number;
};

type EandCScannerPanelProps = {
  viewMode: EandCViewMode;
  onViewModeChange: (mode: EandCViewMode) => void;
  onRefresh: () => void;
  settings: EandCSettings;
  onSettingsChange: (updates: Partial<EandCSettings>) => void;
  onApplySettings: () => void;
  onResetSettings: () => void;
  contractionCount: number;
  expansionCount: number;
  intersectionCount: number;
  eitherCount: number;
};

const VIEW_OPTIONS: Array<{ key: EandCViewMode; label: string }> = [
  { key: "contraction", label: "Contraction" },
  { key: "expansion", label: "Expansion" },
  { key: "both", label: "Both (Either / OR)" },
];

export function EandCScannerPanel({
  viewMode,
  onViewModeChange,
  onRefresh,
  settings,
  onSettingsChange,
  onApplySettings,
  onResetSettings,
  contractionCount,
  expansionCount,
  intersectionCount,
  eitherCount,
}: EandCScannerPanelProps) {
  return (
    <Panel
      title="E&C"
      subtitle="Contraction and expansion setups. Pick one tab, or Both to see the OR union across both lists."
      actions={(
        <div className="custom-panel-actions">
          <button type="button" className="nav-button ghost" onClick={onApplySettings}>
            Apply
          </button>
          <button type="button" className="nav-button ghost" onClick={onResetSettings}>
            Reset
          </button>
          <button type="button" className="nav-button ghost" onClick={onRefresh}>
            Refresh
          </button>
        </div>
      )}
      className="gap-up-panel"
    >
      <div className="gap-up-toolbar">
        <span className="gap-up-label">View</span>
        <div className="gap-up-options">
          {VIEW_OPTIONS.map((option) => (
            <button
              key={option.key}
              type="button"
              className={viewMode === option.key ? "tool-pill active" : "tool-pill"}
              onClick={() => onViewModeChange(option.key)}
            >
              {option.label}
            </button>
          ))}
        </div>
      </div>

      <div className="scanner-settings-note" style={{ marginTop: "0.65rem" }}>
        <strong>
          {`Contraction: ${contractionCount} | Expansion: ${expansionCount} | Either (OR): ${eitherCount} | Intersection: ${intersectionCount}`}
        </strong>
        <span>
          Contraction: tight daily moves with trend support, healthy volume, and a prior run-up. Expansion: strong price+volume expansion days.
        </span>
      </div>

      <div className="scan-settings-grid" style={{ marginTop: "0.85rem" }}>
        <label>
          <span>Min Price</span>
          <input
            type="number"
            min={0}
            step={1}
            value={settings.min_price}
            onChange={(event) => onSettingsChange({ min_price: Number(event.target.value) || 0 })}
          />
        </label>
        <label>
          <span>Contraction Min Avg Vol (50D)</span>
          <input
            type="number"
            min={0}
            step={1000}
            value={settings.contraction_min_avg_volume_50d}
            onChange={(event) => onSettingsChange({ contraction_min_avg_volume_50d: Number(event.target.value) || 0 })}
          />
        </label>
        <label>
          <span>Contraction Min Day Vol</span>
          <input
            type="number"
            min={0}
            step={1000}
            value={settings.contraction_min_day_volume}
            onChange={(event) => onSettingsChange({ contraction_min_day_volume: Number(event.target.value) || 0 })}
          />
        </label>
        <label>
          <span>Contraction Max Today % (abs)</span>
          <input
            type="number"
            min={0}
            step={0.1}
            value={settings.contraction_max_today_change_abs_pct}
            onChange={(event) => onSettingsChange({ contraction_max_today_change_abs_pct: Number(event.target.value) || 0 })}
          />
        </label>
        <label>
          <span>Contraction Max Prev % (abs)</span>
          <input
            type="number"
            min={0}
            step={0.1}
            value={settings.contraction_max_prev_day_change_abs_pct}
            onChange={(event) => onSettingsChange({ contraction_max_prev_day_change_abs_pct: Number(event.target.value) || 0 })}
          />
        </label>
        <label>
          <span>Contraction Max 2D Ago % (abs)</span>
          <input
            type="number"
            min={0}
            step={0.1}
            value={settings.contraction_max_two_days_ago_change_abs_pct}
            onChange={(event) => onSettingsChange({ contraction_max_two_days_ago_change_abs_pct: Number(event.target.value) || 0 })}
          />
        </label>
        <label>
          <span>Contraction Max Price / SMA50</span>
          <input
            type="number"
            min={0.1}
            step={0.01}
            value={settings.contraction_max_price_to_sma50_ratio}
            onChange={(event) => onSettingsChange({ contraction_max_price_to_sma50_ratio: Number(event.target.value) || 0.1 })}
          />
        </label>
        <label>
          <span>Min 5D Return % (prior run-up)</span>
          <input
            type="number"
            min={0}
            step={1}
            value={settings.contraction_min_return_5d}
            onChange={(event) => onSettingsChange({ contraction_min_return_5d: Number(event.target.value) || 0 })}
          />
        </label>
        <label>
          <span>Min 20D Return % (prior run-up)</span>
          <input
            type="number"
            min={0}
            step={1}
            value={settings.contraction_min_return_20d}
            onChange={(event) => onSettingsChange({ contraction_min_return_20d: Number(event.target.value) || 0 })}
          />
        </label>
        <label>
          <span>Min 60D Return % (prior run-up)</span>
          <input
            type="number"
            min={0}
            step={1}
            value={settings.contraction_min_return_60d}
            onChange={(event) => onSettingsChange({ contraction_min_return_60d: Number(event.target.value) || 0 })}
          />
        </label>
        <label>
          <span>Expansion Min Change %</span>
          <input
            type="number"
            step={0.1}
            value={settings.expansion_min_change_pct}
            onChange={(event) => onSettingsChange({ expansion_min_change_pct: Number(event.target.value) || 0 })}
          />
        </label>
        <label>
          <span>Expansion Min Avg Vol (50D)</span>
          <input
            type="number"
            min={0}
            step={1000}
            value={settings.expansion_min_avg_volume_50d}
            onChange={(event) => onSettingsChange({ expansion_min_avg_volume_50d: Number(event.target.value) || 0 })}
          />
        </label>
        <label>
          <span>Expansion Min Day Vol</span>
          <input
            type="number"
            min={0}
            step={1000}
            value={settings.expansion_min_day_volume}
            onChange={(event) => onSettingsChange({ expansion_min_day_volume: Number(event.target.value) || 0 })}
          />
        </label>
        <label>
          <span>Expansion Min Vol Multiple</span>
          <input
            type="number"
            min={0}
            step={0.1}
            value={settings.expansion_min_volume_multiple}
            onChange={(event) => onSettingsChange({ expansion_min_volume_multiple: Number(event.target.value) || 0 })}
          />
        </label>
        <label className="scan-settings-checkbox">
          <span>Require Above EMA50 (Contraction)</span>
          <input
            type="checkbox"
            checked={settings.contraction_require_above_ema50}
            onChange={(event) => onSettingsChange({ contraction_require_above_ema50: event.target.checked })}
          />
        </label>
        <label className="scan-settings-checkbox">
          <span>Require Prior Run-Up (Contraction)</span>
          <input
            type="checkbox"
            checked={settings.contraction_require_prior_run_up}
            onChange={(event) => onSettingsChange({ contraction_require_prior_run_up: event.target.checked })}
          />
        </label>
      </div>
    </Panel>
  );
}
