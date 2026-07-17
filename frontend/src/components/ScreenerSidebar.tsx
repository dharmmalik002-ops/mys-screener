import {
  Flag,
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
  Eye,
  EyeOff,
  Flame,
  Layers,
  LineChart,
  SlidersHorizontal,
  Sparkles,
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
  | "positive-earnings"
  | "episodic-pivot"
  | "rs-line-leads"
  | "fresh-stage2"
  | "high-tight-flag"
  | "vcp"
  | "tight-closes"
  | "power-base";

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

// Full catalog. Which of these actually SHOW in the sidebar is a user
// preference (persisted in localStorage) — the default is the curated
// low-volatility set below; everything else is one toggle away.
const ALL_ITEMS: SidebarItem[] = [
  { mode: "vcp", title: "VCP", hint: "Volatility contraction pattern", Icon: Layers },
  { mode: "power-base", title: "Power Base", hint: "30%+ first leg, now consolidating", Icon: Flame },
  { mode: "tight-closes", title: "3 Tight Closes", hint: "Pre-breakout coil", Icon: Crosshair },
  { mode: "bread-butter", title: "Bread & Butter", hint: "Stage 2 impulse + 10/21 EMA rest", Icon: Trophy },
  { mode: "custom-scan", title: "Custom Scanner", hint: "Build your own", Icon: Settings2 },
  { mode: "volume", title: "Volume", hint: "Recent volume-high pushes", Icon: BarChart3 },
  { mode: "ipo", title: "IPO", hint: "Recent listings", Icon: Rocket },
  { mode: "gap-up-openers", title: "Gap Up Openers", hint: "Opening gaps", Icon: TrendingUp },
  { mode: "ema-expansion", title: "Expansion", hint: "EMA expanding", Icon: Maximize2 },
  { mode: "contraction", title: "Contraction", hint: "Tight ranges", Icon: Minimize2 },
  { mode: "momentum-burst", title: "Momentum Burst", hint: "Bursts + EMA rest setups", Icon: Zap },
  { mode: "positive-earnings", title: "Positive Earnings", hint: "Strong post-result reaction", Icon: Award },
  { mode: "improving-rs", title: "52 Week High RS", hint: "RS 52W high", Icon: Activity },
  { mode: "minervini-1m", title: "Minervini 1 Month", hint: "Trend template", Icon: LineChart },
  { mode: "minervini-5m", title: "Minervini 5 Months", hint: "Mature trend template", Icon: LineChart },
  { mode: "episodic-pivot", title: "Episodic Pivot", hint: "Day-one gap from a flat base", Icon: Sparkles },
  { mode: "rs-line-leads", title: "RS Line Leads", hint: "RS high before the pivot", Icon: Activity },
  { mode: "fresh-stage2", title: "Fresh Stage 2", hint: "New trend-template entrants", Icon: Flag },
  { mode: "high-tight-flag", title: "High Tight Flag", hint: "Steep pole, shallow flag", Icon: Flag },
  { mode: "consolidating", title: "Consolidating", hint: "Long base / multi-year high", Icon: Layers },
  { mode: "near-pivot", title: "Near Pivot", hint: "High-RS names tightening", Icon: Crosshair },
  { mode: "pull-backs", title: "Pull Backs", hint: "Leaders at the 10/20 EMA", Icon: LineChart },
  { mode: "returns", title: "Returns", hint: "Scan by return range", Icon: BarChart3 },
  { mode: "demand-zone", title: "Demand Zone", hint: "Stage 2 at strong demand lows", Icon: Layers },
];

// The curated "what I actually trade" set — low-volatility continuation
// setups plus the freeform builder.
const DEFAULT_VISIBLE_MODES: ScreenerMode[] = [
  "vcp",
  "power-base",
  "tight-closes",
  "bread-butter",
  "custom-scan",
];

const VISIBLE_SCANNERS_KEY = "sidebar-visible-scanners-v1";

function loadVisibleModes(): ScreenerMode[] {
  try {
    const raw = window.localStorage.getItem(VISIBLE_SCANNERS_KEY);
    if (!raw) return DEFAULT_VISIBLE_MODES;
    const parsed = JSON.parse(raw) as unknown;
    if (!Array.isArray(parsed)) return DEFAULT_VISIBLE_MODES;
    const known = new Set(ALL_ITEMS.map((item) => item.mode as string));
    const modes = parsed.filter((m): m is ScreenerMode => typeof m === "string" && known.has(m));
    return modes.length > 0 ? modes : DEFAULT_VISIBLE_MODES;
  } catch {
    return DEFAULT_VISIBLE_MODES;
  }
}

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
  const [visibleModes, setVisibleModes] = useState<ScreenerMode[]>(() => loadVisibleModes());
  const [managing, setManaging] = useState(false);

  const toggleModeVisibility = (mode: ScreenerMode) => {
    setVisibleModes((current) => {
      const next = current.includes(mode)
        ? current.filter((m) => m !== mode)
        : [...current, mode];
      try {
        window.localStorage.setItem(VISIBLE_SCANNERS_KEY, JSON.stringify(next));
      } catch {
        // storage full/blocked — the toggle still works for this session
      }
      return next;
    });
  };

  // Manage mode lists everything; normal mode lists the visible set, plus the
  // active scanner even when hidden (never strand the user's current view).
  const filteredItems = managing
    ? ALL_ITEMS
    : ALL_ITEMS.filter((item) => visibleModes.includes(item.mode) || item.mode === activeMode);

  const handleNewScreener = () => {
    // "New Screener" defaults to opening the Custom Scanner (the only freeform builder)
    onModeChange("custom-scan");
  };

  return (
    <Panel
      title="Screener"
      subtitle={`${visibleModes.length} of ${ALL_ITEMS.length} scanners · ${savedScanners.length} saved`}
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
            <button
              type="button"
              className={`ss-manage-btn${managing ? " is-active" : ""}`}
              onClick={() => setManaging((v) => !v)}
              title={managing ? "Done managing scanners" : "Show / hide scanners"}
              aria-label={managing ? "Done managing scanners" : "Show or hide scanners"}
            >
              {managing ? <span>Done</span> : <SlidersHorizontal size={13} strokeWidth={2.2} />}
            </button>
          </div>
          {managing ? (
            <div className="ss-manage-hint">Click the eye to show or hide a scanner.</div>
          ) : null}
          {filteredItems.length === 0 ? (
            <div className="ss-empty">No scanners match.</div>
          ) : (
            filteredItems.map((item) => {
              const Icon = item.Icon;
              const matchingSaved = savedScanners.filter((saved) => saved.mode === item.mode);
              const isActive = activeMode === item.mode;
              const isVisible = visibleModes.includes(item.mode);
              const count = counts[item.mode] ?? 0;
              return (
                <div key={item.mode} className={`ss-nav-group${managing && !isVisible ? " is-hidden-scanner" : ""}`}>
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
                    {managing ? (
                      <span
                        role="button"
                        tabIndex={0}
                        className={`ss-eye-toggle${isVisible ? " is-on" : ""}`}
                        title={isVisible ? "Hide from sidebar" : "Show in sidebar"}
                        aria-label={isVisible ? `Hide ${item.title}` : `Show ${item.title}`}
                        onClick={(event) => {
                          event.stopPropagation();
                          toggleModeVisibility(item.mode);
                        }}
                        onKeyDown={(event) => {
                          if (event.key === "Enter" || event.key === " ") {
                            event.preventDefault();
                            event.stopPropagation();
                            toggleModeVisibility(item.mode);
                          }
                        }}
                      >
                        {isVisible ? <Eye size={14} strokeWidth={2.2} /> : <EyeOff size={14} strokeWidth={2.2} />}
                      </span>
                    ) : (
                      <span className="ss-nav-count">{count}</span>
                    )}
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
