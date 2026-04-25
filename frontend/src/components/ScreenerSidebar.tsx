import { useMemo, useState } from "react";
import {
  Plus,
  Search,
  Sparkles,
  Settings2,
  Rocket,
  TrendingUp,
  Maximize2,
  Minimize2,
  Crosshair,
  Undo2,
  Activity,
  BarChart3,
  Layers,
  Crown,
  Trash2,
} from "lucide-react";

import type { MarketKey } from "../lib/api";
import { Panel } from "./Panel";

import "./ScreenerSidebar.css";

export type ScreenerMode =
  | "custom-scan"
  | "ipo"
  | "gap-up-openers"
  | "ema-expansion"
  | "contraction"
  | "near-pivot"
  | "pull-backs"
  | "improving-rs"
  | "returns"
  | "consolidating"
  | "minervini-1m"
  | "minervini-5m";

export type SavedSidebarScanner = {
  id: string;
  name: string;
  mode: Exclude<ScreenerMode, "improving-rs">;
  lastMatchCount?: number;
  lastUpdatedAt?: string | null;
  isStale?: boolean;
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

type SidebarItem = {
  mode: ScreenerMode;
  title: string;
  hint: string;
  Icon: typeof Plus;
};

const ITEMS: SidebarItem[] = [
  { mode: "custom-scan", title: "Custom Scanner", hint: "Build your own", Icon: Settings2 },
  { mode: "ipo", title: "IPO", hint: "Recent listings", Icon: Rocket },
  { mode: "gap-up-openers", title: "Gap Up Openers", hint: "Opening gaps", Icon: TrendingUp },
  { mode: "ema-expansion", title: "Expansion", hint: "EMA expanding", Icon: Maximize2 },
  { mode: "contraction", title: "Contraction", hint: "Tight ranges", Icon: Minimize2 },
  { mode: "minervini-1m", title: "Minervini 1 Month", hint: "Trend template", Icon: Crosshair },
  { mode: "minervini-5m", title: "Minervini 5 Months", hint: "Long uptrend", Icon: Crosshair },
  { mode: "improving-rs", title: "Improving RS", hint: "Relative strength", Icon: Activity },
];

const QUICK_FILTERS: Array<{ label: string; query: string }> = [
  { label: "Market Cap > 1000 Cr", query: "market" },
  { label: "Volume Surge", query: "gap" },
  { label: "RS > 80", query: "rs" },
  { label: "Tight Contraction", query: "contraction" },
  { label: "Recent IPOs", query: "ipo" },
];

export function ScreenerSidebar({
  market: _market,
  activeMode,
  onModeChange,
  counts,
  savedScanners,
  activeSavedScannerId,
  onLoadSavedScanner,
  onDeleteSavedScanner,
}: ScreenerSidebarProps) {
  const [search, setSearch] = useState("");
  const [activeChip, setActiveChip] = useState<string | null>(null);

  const filteredItems = useMemo(() => {
    const needle = (activeChip ?? search).trim().toLowerCase();
    if (!needle) return ITEMS;
    return ITEMS.filter(
      (it) =>
        it.title.toLowerCase().includes(needle) ||
        it.hint.toLowerCase().includes(needle) ||
        it.mode.toLowerCase().includes(needle),
    );
  }, [search, activeChip]);

  const handleNewScreener = () => {
    // "New Screener" defaults to opening the Custom Scanner (the only freeform builder)
    onModeChange("custom-scan");
  };

  const handleChipClick = (query: string) => {
    setActiveChip((prev) => (prev === query ? null : query));
    setSearch("");
  };

  return (
    <Panel
      title="Screener"
      subtitle="Discover and save your edge"
      className="screener-sidebar-panel ss-shell"
    >
      <div className="ss-root">
        {/* New Screener CTA */}
        <button type="button" className="ss-new-btn" onClick={handleNewScreener}>
          <Plus size={16} strokeWidth={2.4} />
          <span>New Screener</span>
        </button>

        {/* Quick Filters */}
        <section className="ss-quick">
          <div className="ss-section-label">
            <Sparkles size={13} />
            <span>Quick Filters</span>
          </div>
          <div className="ss-search">
            <Search size={14} className="ss-search-icon" />
            <input
              type="search"
              className="ss-search-input"
              placeholder="Search scanners…"
              value={search}
              onChange={(e) => {
                setSearch(e.target.value);
                if (e.target.value) setActiveChip(null);
              }}
            />
          </div>
          <div className="ss-chips">
            {QUICK_FILTERS.map((qf) => (
              <button
                key={qf.label}
                type="button"
                className={`ss-chip${activeChip === qf.query ? " ss-chip-active" : ""}`}
                onClick={() => handleChipClick(qf.query)}
              >
                {qf.label}
              </button>
            ))}
          </div>
        </section>

        {/* Scanner list */}
        <nav className="ss-nav" aria-label="Scanner modes">
          <div className="ss-section-label ss-section-label-spaced">
            <Layers size={13} />
            <span>Scanners</span>
          </div>
          {filteredItems.length === 0 ? (
            <div className="ss-empty">No scanners match.</div>
          ) : (
            filteredItems.map((item) => {
              const Icon = item.Icon;
              const matchingSaved = savedScanners.filter((saved) => saved.mode === item.mode);
              const isActive = activeMode === item.mode;
              const count = counts[item.mode] ?? 0;
              return (
                <div key={item.mode} className="ss-nav-group">
                  <button
                    type="button"
                    className={`ss-nav-item${isActive ? " is-active" : ""}`}
                    onClick={() => onModeChange(item.mode)}
                  >
                    <span className="ss-nav-icon" aria-hidden>
                      <Icon size={16} strokeWidth={2} />
                    </span>
                    <span className="ss-nav-text">
                      <strong>{item.title}</strong>
                      <small>{item.hint}</small>
                    </span>
                    <span className="ss-nav-count">{count}</span>
                  </button>

                  {matchingSaved.length > 0 ? (
                    <div className="ss-saved-list">
                      {matchingSaved.map((saved) => (
                        <div
                          key={saved.id}
                          className={`ss-saved-item${activeSavedScannerId === saved.id ? " is-active" : ""}`}
                        >
                          <button
                            type="button"
                            className="ss-saved-load"
                            onClick={() => onLoadSavedScanner(saved.id)}
                            title={saved.name}
                          >
                            <BarChart3 size={12} className="ss-saved-icon" />
                            <span className="ss-saved-text">
                              <strong>
                                {saved.name}
                                {saved.isStale ? <span className="ss-stale">Stale</span> : null}
                              </strong>
                              <small>{saved.lastMatchCount ?? 0} stocks</small>
                            </span>
                          </button>
                          <button
                            type="button"
                            className="ss-saved-delete"
                            onClick={() => onDeleteSavedScanner(saved.id)}
                            aria-label={`Remove ${saved.name}`}
                          >
                            <Trash2 size={13} />
                          </button>
                        </div>
                      ))}
                    </div>
                  ) : null}
                </div>
              );
            })
          )}
        </nav>

        {/* Pro Features upgrade card */}
        <div className="ss-pro-card">
          <div className="ss-pro-head">
            <span className="ss-pro-crown" aria-hidden>
              <Crown size={14} strokeWidth={2.2} />
            </span>
            <strong>Pro Features</strong>
          </div>
          <p className="ss-pro-copy">
            Unlock AI-driven screens, real-time alerts and unlimited saved scanners.
          </p>
          <ul className="ss-pro-bullets">
            <li>
              <Undo2 size={11} /> Backtest any preset
            </li>
            <li>
              <Activity size={11} /> Live RS &amp; volume signals
            </li>
          </ul>
          <button
            type="button"
            className="ss-pro-cta"
            onClick={() => {
              if (typeof window !== "undefined") {
                window.alert("Upgrade flow coming soon — Pro features are in private beta.");
              }
            }}
          >
            Upgrade Now
          </button>
        </div>
      </div>
    </Panel>
  );
}
