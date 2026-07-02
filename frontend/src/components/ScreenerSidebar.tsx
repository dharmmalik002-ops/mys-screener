import {
  Plus,
  Settings2,
  Rocket,
  TrendingUp,
  Maximize2,
  Minimize2,
  Crosshair,
  Activity,
  Award,
  BarChart3,
  Layers,
  Trash2,
  Trophy,
  Zap,
} from "lucide-react";

import { useEffect, useState } from "react";

import { getScannerScorecard, type MarketKey, type ScannerScorecardRow } from "../lib/api";
import { Panel } from "./Panel";

import "./ScreenerSidebar.css";

export type ScreenerMode =
  | "bread-butter"
  | "custom-scan"
  | "volume"
  | "ipo"
  | "gap-up-openers"
  | "ema-expansion"
  | "contraction"
  | "near-pivot"
  | "pull-backs"
  | "improving-rs"
  | "returns"
  | "consolidating"
  | "demand-zone"
  | "momentum-burst"
  | "minervini-1m"
  | "minervini-5m"
  | "positive-earnings";

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
  { mode: "bread-butter", title: "Bread & Butter", hint: "Stage 2 impulse + 10/21 EMA rest", Icon: Trophy },
  { mode: "custom-scan", title: "Custom Scanner", hint: "Build your own", Icon: Settings2 },
  { mode: "volume", title: "Volume", hint: "Recent volume-high pushes", Icon: BarChart3 },
  { mode: "ipo", title: "IPO", hint: "Recent listings", Icon: Rocket },
  { mode: "gap-up-openers", title: "Gap Up Openers", hint: "Opening gaps", Icon: TrendingUp },
  { mode: "ema-expansion", title: "Expansion", hint: "EMA expanding", Icon: Maximize2 },
  { mode: "contraction", title: "Contraction", hint: "Tight ranges", Icon: Minimize2 },
  { mode: "demand-zone", title: "Demand Zone Scanner", hint: "Daily/weekly demand", Icon: Layers },
  { mode: "momentum-burst", title: "Momentum Burst", hint: "Bursts + EMA rest setups", Icon: Zap },
  { mode: "minervini-1m", title: "Minervini 1 Month", hint: "Trend template", Icon: Crosshair },
  { mode: "minervini-5m", title: "Minervini 5 Months", hint: "Long uptrend", Icon: Crosshair },
  { mode: "positive-earnings", title: "Positive Earnings", hint: "Strong post-result reaction", Icon: Award },
  { mode: "improving-rs", title: "52 Week High RS", hint: "RS 52W high", Icon: Activity },
];

function ScannerScorecard({ market }: { market: MarketKey }) {
  const [rows, setRows] = useState<ScannerScorecardRow[] | null>(null);
  const [open, setOpen] = useState(true);

  useEffect(() => {
    let active = true;
    getScannerScorecard(market)
      .then((response) => {
        if (active) setRows(response.rows.filter((row) => row.hits > 0));
      })
      .catch(() => {
        if (active) setRows([]);
      });
    return () => {
      active = false;
    };
  }, [market]);

  if (rows === null || rows.length === 0) {
    // No forward history yet (it accumulates daily) — keep the rail clean.
    return null;
  }

  return (
    <div className="ss-scorecard">
      <button type="button" className="ss-section-label ss-scorecard-head" onClick={() => setOpen((v) => !v)}>
        <Trophy size={13} />
        <span>Scanner Scorecard</span>
        <small>{open ? "hide" : "show"}</small>
      </button>
      {open ? (
        <div className="ss-scorecard-rows">
          {rows.map((row) => (
            <div
              key={row.scan_id}
              className="ss-scorecard-row"
              title={
                `${row.hits} picks across ${row.sessions} sessions.` +
                (row.best_symbol ? ` Best: ${row.best_symbol} ${row.best_return_pct}%.` : "") +
                (row.worst_symbol ? ` Worst: ${row.worst_symbol} ${row.worst_return_pct}%.` : "")
              }
            >
              <span className="ss-scorecard-name">{row.scan_name}</span>
              <span className={`ss-scorecard-ret${(row.avg_forward_return_pct ?? 0) >= 0 ? " pos" : " neg"}`}>
                {(row.avg_forward_return_pct ?? 0) >= 0 ? "+" : ""}
                {row.avg_forward_return_pct?.toFixed(1)}%
              </span>
              <span className="ss-scorecard-win">{row.win_rate_pct?.toFixed(0)}% win</span>
            </div>
          ))}
          <small className="ss-scorecard-foot">Avg return of past picks since their scan day.</small>
        </div>
      ) : null}
    </div>
  );
}

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
  const filteredItems = ITEMS;

  const handleNewScreener = () => {
    // "New Screener" defaults to opening the Custom Scanner (the only freeform builder)
    onModeChange("custom-scan");
  };

  return (
    <Panel
      title="Screener"
      subtitle={`${ITEMS.length} scanners · ${savedScanners.length} saved`}
      className="screener-sidebar-panel ss-shell"
    >
      <div className="ss-root">
        {/* New Screener CTA */}
        <button type="button" className="ss-new-btn" onClick={handleNewScreener}>
          <Plus size={16} strokeWidth={2.4} />
          <span>New Screener</span>
        </button>

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

        <ScannerScorecard market={_market} />
      </div>
    </Panel>
  );
}
