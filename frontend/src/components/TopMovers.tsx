import type { ScanMatch } from "../lib/api";
import { Panel } from "./Panel";

type MoversProps = {
  gainers: ScanMatch[];
  losers: ScanMatch[];
  volume: ScanMatch[];
  onPickSymbol: (symbol: string) => void;
};

type ColumnProps = {
  title: string;
  items: ScanMatch[];
  tone: "positive" | "negative" | "neutral";
  onPickSymbol: (symbol: string) => void;
};

function MoversColumn({ title, items, tone, onPickSymbol }: ColumnProps) {
  return (
    <div className="mover-column">
      <div className="mover-column-head">
        <span className={`tone-dot ${tone}`} />
        <h3>{title}</h3>
      </div>
      <div className="mover-list">
        {items.map((item) => (
          <button key={`${title}-${item.symbol}`} type="button" className="mover-row" onClick={() => onPickSymbol(item.symbol)}>
            <strong>{item.symbol}</strong>
            <span className={item.change_pct >= 0 ? "positive-text" : "negative-text"}>
              {title === "Vol Spikes" ? `${item.relative_volume.toFixed(1)}x` : `${item.change_pct.toFixed(2)}%`}
            </span>
          </button>
        ))}
      </div>
    </div>
  );
}

export function TopMovers({ gainers, losers, volume, onPickSymbol }: MoversProps) {
  return (
    <Panel title="Live Movers" subtitle="Free-first feed">
      <div className="movers-grid">
        <MoversColumn title="Top Gainers" items={gainers} tone="positive" onPickSymbol={onPickSymbol} />
        <MoversColumn title="Top Losers" items={losers} tone="negative" onPickSymbol={onPickSymbol} />
        <MoversColumn title="Vol Spikes" items={volume} tone="neutral" onPickSymbol={onPickSymbol} />
      </div>
    </Panel>
  );
}

