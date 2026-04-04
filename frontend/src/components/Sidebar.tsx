import { type ChangeEvent } from "react";

import type { ScanDescriptor } from "../lib/api";
import { SCANNER_BADGES } from "../lib/scannerCatalog";

type SidebarProps = {
  scanners: ScanDescriptor[];
  selectedScanId: string;
  search: string;
  onSearchChange: (value: string) => void;
  onSelectScan: (scanId: string) => void;
  threshold: number;
  universeCount: number;
};

export function Sidebar({
  scanners,
  selectedScanId,
  search,
  onSearchChange,
  onSelectScan,
  threshold,
  universeCount,
}: SidebarProps) {
  const handleSearch = (event: ChangeEvent<HTMLInputElement>) => {
    onSearchChange(event.target.value);
  };

  return (
    <aside className="sidebar">
      <div className="brand-block">
        <div className="brand-mark">MM</div>
        <div>
          <h1>Mr. Malik Scanner</h1>
          <p>Stock Scanner</p>
        </div>
      </div>

      <div className="sidebar-meta">
        <div>
          <strong>{universeCount}</strong>
          <span>symbols loaded</span>
        </div>
        <div>
          <strong>{threshold} Cr+</strong>
          <span>market cap floor</span>
        </div>
      </div>

      <label className="search-box">
        <span>Search scans</span>
        <input value={search} onChange={handleSearch} placeholder="Breakouts, volume, RS..." />
      </label>

      <div className="scanner-groups">
        <p className="scanner-group-title">Scanners</p>
        {scanners.map((scanner) => (
          <button
            key={scanner.id}
            type="button"
            className={scanner.id === selectedScanId ? "scanner-pill active" : "scanner-pill"}
            onClick={() => onSelectScan(scanner.id)}
          >
            <span className="scanner-pill-name">
              <strong className="scanner-pill-badge">{SCANNER_BADGES[scanner.id] ?? "SCN"}</strong>
              <span>{scanner.name}</span>
            </span>
            <small>{scanner.hit_count}</small>
          </button>
        ))}
      </div>
    </aside>
  );
}
