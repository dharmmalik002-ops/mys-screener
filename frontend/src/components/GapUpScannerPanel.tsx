import { Panel } from "./Panel";

type GapUpScannerPanelProps = {
  threshold: number;
  onThresholdChange: (threshold: number) => void;
  minLiquidityCrore: number | null;
  onMinLiquidityCroreChange: (value: number | null) => void;
};

const GAP_OPTIONS = [1, 2, 3];

export function GapUpScannerPanel({
  threshold,
  onThresholdChange,
  minLiquidityCrore,
  onMinLiquidityCroreChange,
}: GapUpScannerPanelProps) {
  return (
    <Panel
      title="Gap Up Openers"
      subtitle="Find stocks that opened above the previous close by a selected percentage."
      className="gap-up-panel"
    >
      <div className="gap-up-toolbar">
        <span className="gap-up-label">Gap Filter</span>
        <div className="gap-up-options">
          {GAP_OPTIONS.map((value) => (
            <button
              key={value}
              type="button"
              className={value === threshold ? "tool-pill active" : "tool-pill"}
              onClick={() => onThresholdChange(value)}
            >
              {`>${value}%`}
            </button>
          ))}
        </div>
        <label className="scanner-field">
          <span>Min Liquidity (30D Avg Traded Value, Cr)</span>
          <input
            type="number"
            min="0"
            step="1"
            value={minLiquidityCrore ?? ""}
            onChange={(event) => onMinLiquidityCroreChange(event.target.value === "" ? null : Math.max(0, Number(event.target.value)))}
          />
        </label>
      </div>
    </Panel>
  );
}
