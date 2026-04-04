import type { MarketKey } from "../lib/api";
import { Panel } from "./Panel";

export type ScreenerMode = "custom-scan" | "ipo" | "gap-up-openers" | "near-pivot" | "pull-backs" | "improving-rs" | "returns" | "consolidating" | "minervini-1m" | "minervini-5m";
export type SavedSidebarScanner = {
  id: string;
  name: string;
  mode: Exclude<ScreenerMode, "improving-rs">;
  lastMatchCount?: number;
};

type ScreenerSidebarProps = {
  market: MarketKey;
  activeMode: ScreenerMode;
  onModeChange: (mode: ScreenerMode) => void;
  counts: Partial<Record<ScreenerMode, number>>;
  savedScanners: SavedSidebarScanner[];
  activeSavedScannerId: string | null;
  onLoadSavedScanner: (id: string) => void;
  onDeleteSavedScanner: (id: string) => void;
};

const ITEMS: Array<{
  mode: ScreenerMode;
  title: string;
}> = [
  {
    mode: "custom-scan",
    title: "Custom Scanner",
  },
  {
    mode: "ipo",
    title: "IPO",
  },
  {
    mode: "gap-up-openers",
    title: "Gap Up Openers",
  },
  {
    mode: "near-pivot",
    title: "Near Pivot",
  },
  {
    mode: "pull-backs",
    title: "Pull Backs",
  },
  {
    mode: "returns",
    title: "Returns",
  },
  {
    mode: "consolidating",
    title: "Consolidating",
  },
  {
    mode: "minervini-1m",
    title: "Minervini 1 Month",
  },
  {
    mode: "minervini-5m",
    title: "Minervini 5 Months",
  },
  {
    mode: "improving-rs",
    title: "Improving RS",
  },
];

export function ScreenerSidebar({
  market,
  activeMode,
  onModeChange,
  counts,
  savedScanners,
  activeSavedScannerId,
  onLoadSavedScanner,
  onDeleteSavedScanner,
}: ScreenerSidebarProps) {
  const marketLabel = market === "india" ? "India" : "US";

  return (
    <Panel
      title="Screener"
      subtitle={`${marketLabel} scans and saved setups`}
      className="screener-sidebar-panel"
    >
      <div className="screener-nav">
        {ITEMS.map((item) => {
          const matchingSaved = savedScanners.filter((saved) => saved.mode === item.mode);
          return (
            <div key={item.mode} className="screener-nav-group">
              <button
                type="button"
                className={activeMode === item.mode ? "screener-nav-button active" : "screener-nav-button"}
                onClick={() => onModeChange(item.mode)}
              >
                <span className="screener-nav-main">
                  <strong>{item.title}</strong>
                </span>
                <span className="screener-nav-count">{counts[item.mode] ?? 0}</span>
              </button>

              {matchingSaved.length > 0 ? (
                <div className="screener-saved-list">
                  {matchingSaved.map((saved) => (
                    <div
                      key={saved.id}
                      className={activeSavedScannerId === saved.id ? "screener-saved-item active" : "screener-saved-item"}
                    >
                      <button type="button" className="screener-saved-load" onClick={() => onLoadSavedScanner(saved.id)}>
                        <strong>{saved.name}</strong>
                        <small>{saved.lastMatchCount ?? 0} stocks</small>
                      </button>
                      <button type="button" className="screener-saved-delete" onClick={() => onDeleteSavedScanner(saved.id)}>
                        Remove
                      </button>
                    </div>
                  ))}
                </div>
              ) : null}
            </div>
          );
        })}
      </div>
    </Panel>
  );
}
