import { Panel } from "./Panel";

type IpoScannerPanelProps = {
  minLiquidityCrore: number | null;
  onMinLiquidityCroreChange: (value: number | null) => void;
  onApply: () => void;
  onReset: () => void;
};

// Quick picks for the values actually worth typing here. Fresh listings trade
// thin for weeks, so the useful cut is small — anything above ~10 Cr empties
// the list on most sessions.
const LIQUIDITY_PRESETS = [1, 2, 5, 10];

export function IpoScannerPanel({
  minLiquidityCrore,
  onMinLiquidityCroreChange,
  onApply,
  onReset,
}: IpoScannerPanelProps) {
  return (
    <Panel
      title="IPO"
      subtitle="Stocks listed within the last 1 year, newest debuts first."
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
      <div className="gap-up-toolbar">
        <span className="gap-up-label">Min Liquidity</span>
        <div className="gap-up-options">
          <button
            type="button"
            className={minLiquidityCrore === null ? "tool-pill active" : "tool-pill"}
            onClick={() => onMinLiquidityCroreChange(null)}
          >
            Any
          </button>
          {LIQUIDITY_PRESETS.map((value) => (
            <button
              key={value}
              type="button"
              className={value === minLiquidityCrore ? "tool-pill active" : "tool-pill"}
              onClick={() => onMinLiquidityCroreChange(value)}
            >
              {`${value} Cr`}
            </button>
          ))}
        </div>
        <label className="scanner-field">
          <span>Min Liquidity (30D Avg Traded Value, Cr)</span>
          <input
            type="number"
            min="0"
            step="0.5"
            value={minLiquidityCrore ?? ""}
            placeholder="Any"
            onChange={(event) =>
              onMinLiquidityCroreChange(event.target.value === "" ? null : Math.max(0, Number(event.target.value)))
            }
            onKeyDown={(event) => {
              if (event.key === "Enter") onApply();
            }}
          />
          <small>Leave blank to list every listing from the last year</small>
        </label>
      </div>
    </Panel>
  );
}
