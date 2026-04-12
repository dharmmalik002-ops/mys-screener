import { Panel } from "./Panel";

export type EandCViewMode = "expansion";

export type EandCSettings = {
  expansion_min_change_pct: number;
  expansion_min_relative_volume: number;
  expansion_min_day_volume: number;
};

type EandCScannerPanelProps = {
  onRefresh: () => void;
  settings: EandCSettings;
  onSettingsChange: (updates: Partial<EandCSettings>) => void;
  onApplySettings: () => void;
  onResetSettings: () => void;
  expansionCount: number;
};

export function EandCScannerPanel({
  onRefresh,
  settings,
  onSettingsChange,
  onApplySettings,
  onResetSettings,
  expansionCount,
}: EandCScannerPanelProps) {
  return (
    <Panel
      title="Expansion"
      subtitle="Fresh scanner: day change >= 6%, RVOL >= 2, and same-day volume above 50,000."
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
      <div className="scanner-settings-note" style={{ marginTop: "0.65rem" }}>
        <strong>{`Expansion Matches: ${expansionCount}`}</strong>
        <span>A stock qualifies only when all three rules pass on the same day.</span>
      </div>

      <div className="scan-settings-grid" style={{ marginTop: "0.85rem" }}>
        <label>
          <span>Min Day Change %</span>
          <input
            type="number"
            min={0}
            step={0.1}
            value={settings.expansion_min_change_pct}
            onChange={(event) => onSettingsChange({ expansion_min_change_pct: Number(event.target.value) || 0 })}
          />
        </label>
        <label>
          <span>Min RVOL</span>
          <input
            type="number"
            min={0}
            step={0.1}
            value={settings.expansion_min_relative_volume}
            onChange={(event) => onSettingsChange({ expansion_min_relative_volume: Number(event.target.value) || 0 })}
          />
        </label>
        <label>
          <span>Min Day Volume</span>
          <input
            type="number"
            min={0}
            step={1000}
            value={settings.expansion_min_day_volume}
            onChange={(event) => onSettingsChange({ expansion_min_day_volume: Number(event.target.value) || 0 })}
          />
        </label>
      </div>
    </Panel>
  );
}
