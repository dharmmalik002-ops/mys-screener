import { Panel } from "./Panel";

export type EandCViewMode = "contraction" | "expansion" | "both";

type EandCScannerPanelProps = {
  viewMode: EandCViewMode;
  onViewModeChange: (mode: EandCViewMode) => void;
  onRefresh: () => void;
  contractionCount: number;
  expansionCount: number;
  intersectionCount: number;
};

const VIEW_OPTIONS: Array<{ key: EandCViewMode; label: string }> = [
  { key: "contraction", label: "Contraction" },
  { key: "expansion", label: "Expansion" },
  { key: "both", label: "Both (Intersection)" },
];

export function EandCScannerPanel({
  viewMode,
  onViewModeChange,
  onRefresh,
  contractionCount,
  expansionCount,
  intersectionCount,
}: EandCScannerPanelProps) {
  return (
    <Panel
      title="E&C"
      subtitle="Contraction and expansion setups. Pick one tab, or Both to see the overlap between the two lists."
      actions={(
        <div className="custom-panel-actions">
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
          {`Contraction: ${contractionCount} | Expansion: ${expansionCount} | Intersection: ${intersectionCount}`}
        </strong>
        <span>
          Contraction: tight daily moves with trend support and healthy volume. Expansion: strong price+volume expansion days.
        </span>
      </div>
    </Panel>
  );
}
