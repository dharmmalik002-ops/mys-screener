import { Panel } from "./Panel";

type MinerviniScannerPanelProps = {
  title: string;
  subtitle: string;
  minLiquidityCrore: number | null;
  onMinLiquidityCroreChange: (value: number | null) => void;
  onApply: () => void;
  onReset: () => void;
};

export function MinerviniScannerPanel({
  title,
  subtitle,
  minLiquidityCrore,
  onMinLiquidityCroreChange,
  onApply,
  onReset,
}: MinerviniScannerPanelProps) {
  return (
    <Panel
      title={title}
      subtitle={subtitle}
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
          <span>Min Liquidity (30D Avg Traded Value, Cr)</span>
          <input
            type="number"
            min="0"
            step="1"
            value={minLiquidityCrore ?? ""}
            onChange={(event) =>
              onMinLiquidityCroreChange(event.target.value === "" ? null : Math.max(0, Number(event.target.value)))
            }
          />
          <small>Leave blank to keep the raw Minervini rule set unchanged</small>
        </label>
      </div>
    </Panel>
  );
}
