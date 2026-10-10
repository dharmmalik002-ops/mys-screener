import { useEffect, useRef, type ReactNode } from "react";

import { ScreenerLayoutToggle, type ScreenerLayout } from "./ScreenerLayoutToggle";
import { ALL_ITEMS, type ScreenerMode, type SavedSidebarScanner } from "./ScreenerSidebar";

import "./ResearchStockList.css";

export type ResearchListRow = {
  symbol: string;
  name?: string | null;
  last: number | null;
  changePct: number | null;
  /** Relative volume (today vs the 20-day average); scans carry no raw volume. */
  rvol: number | null;
  /** Shown in the fourth column instead of RVol when the list sets `extraLabel`. */
  extraText?: string | null;
  extraTone?: "up" | "down" | null;
};

type ResearchStockListProps = {
  activeMode?: ScreenerMode;
  onModeChange?: (mode: ScreenerMode) => void;
  savedScanners?: SavedSidebarScanner[];
  activeSavedScannerId?: string | null;
  onLoadSavedScanner?: (id: string) => void;
  /** Replaces the screener dropdown — the AI Scanner heads the list with its own query. */
  head?: ReactNode;
  /** Fourth column header; rows then show `extraText` instead of RVol. */
  extraLabel?: string;
  emptyText?: string;
  ariaLabel?: string;
  rows: ResearchListRow[];
  loading: boolean;
  selectedSymbol: string | null;
  onSelect: (symbol: string) => void;
  onPrefetch?: (symbol: string) => void;
  onLayoutChange: (next: ScreenerLayout) => void;
  sessionLabel?: string | null;
};

function formatPrice(value: number | null) {
  if (value == null || !Number.isFinite(value)) return "—";
  return value.toLocaleString("en-IN", { minimumFractionDigits: value < 100 ? 2 : 1, maximumFractionDigits: 2 });
}

function formatChange(value: number | null) {
  if (value == null || !Number.isFinite(value)) return "—";
  return `${value > 0 ? "+" : ""}${value.toFixed(2)}%`;
}

function formatRvol(value: number | null) {
  if (value == null || !Number.isFinite(value) || value <= 0) return "—";
  return `${value.toFixed(value >= 10 ? 0 : 1)}x`;
}

/**
 * The research layout's left rail: pick a screener from the dropdown, walk its
 * stocks. Selecting a row only changes the symbol — the chart and the
 * fundamentals pane beside it follow; nothing opens on top.
 */
export function ResearchStockList({
  activeMode = "custom-scan",
  onModeChange,
  savedScanners = [],
  activeSavedScannerId = null,
  onLoadSavedScanner,
  head,
  extraLabel,
  emptyText = "No stocks match this screener.",
  ariaLabel = "Screener stocks",
  rows,
  loading,
  selectedSymbol,
  onSelect,
  onPrefetch,
  onLayoutChange,
  sessionLabel,
}: ResearchStockListProps) {
  const listRef = useRef<HTMLDivElement | null>(null);

  // Arrow keys move the selection from App; keep the selected row in view.
  useEffect(() => {
    if (!selectedSymbol || !listRef.current) return;
    const row = listRef.current.querySelector<HTMLElement>(`[data-symbol="${CSS.escape(selectedSymbol)}"]`);
    row?.scrollIntoView({ block: "nearest" });
  }, [selectedSymbol, rows]);

  const selectValue = activeSavedScannerId ? `saved:${activeSavedScannerId}` : `mode:${activeMode}`;

  return (
    <aside className="research-list" aria-label={ariaLabel}>
      <div className="research-list-head">
        {head ?? (
        <select
          className="research-list-select"
          value={selectValue}
          aria-label="Choose a screener"
          onChange={(event) => {
            const value = event.target.value;
            if (value.startsWith("saved:")) onLoadSavedScanner?.(value.slice(6));
            else onModeChange?.(value.slice(5) as ScreenerMode);
            event.currentTarget.blur();
          }}
        >
          <optgroup label="Screeners">
            {ALL_ITEMS.map((item) => (
              <option key={item.mode} value={`mode:${item.mode}`}>
                {item.title}
                {!activeSavedScannerId && item.mode === activeMode && !loading ? ` (${rows.length})` : ""}
              </option>
            ))}
          </optgroup>
          {savedScanners.length > 0 ? (
            <optgroup label="Saved">
              {savedScanners.map((item) => (
                <option key={item.id} value={`saved:${item.id}`}>
                  {item.name}
                  {item.id === activeSavedScannerId && !loading ? ` (${rows.length})` : ""}
                </option>
              ))}
            </optgroup>
          ) : null}
        </select>
        )}
      </div>
      <div className="research-list-switch">
        <ScreenerLayoutToggle value="research" onChange={onLayoutChange} />
      </div>

      <div className="research-list-cols" aria-hidden="true">
        <span>Symbol</span>
        <span>Last</span>
        <span>Chg%</span>
        <span>{extraLabel ?? "RVol"}</span>
      </div>

      <div className="research-list-body" ref={listRef} role="listbox" aria-label="Stocks">
        {loading && rows.length === 0 ? (
          <div className="research-list-empty">Loading…</div>
        ) : rows.length === 0 ? (
          <div className="research-list-empty">{emptyText}</div>
        ) : (
          rows.map((row) => {
            const active = row.symbol === selectedSymbol;
            const tone = row.changePct == null ? "" : row.changePct > 0 ? " up" : row.changePct < 0 ? " down" : "";
            return (
              <button
                key={row.symbol}
                type="button"
                role="option"
                aria-selected={active}
                data-symbol={row.symbol}
                className={active ? "research-list-row active" : "research-list-row"}
                title={row.name ?? row.symbol}
                onClick={() => onSelect(row.symbol)}
                onMouseEnter={onPrefetch ? () => onPrefetch(row.symbol) : undefined}
              >
                <span className="research-list-sym">
                  {row.symbol}
                  {row.name ? <span className="research-list-name">{row.name}</span> : null}
                </span>
                <span className="research-list-num">{formatPrice(row.last)}</span>
                <span className={`research-list-num${tone}`}>{formatChange(row.changePct)}</span>
                {extraLabel ? (
                  <span className={`research-list-num${row.extraTone ? ` ${row.extraTone}` : " muted"}`}>{row.extraText ?? "—"}</span>
                ) : (
                  <span className="research-list-num muted">{formatRvol(row.rvol)}</span>
                )}
              </button>
            );
          })
        )}
      </div>

      <div className="research-list-foot">
        <span>↑ / ↓ to walk the list</span>
        {sessionLabel ? <span>close {sessionLabel}</span> : null}
      </div>
    </aside>
  );
}
