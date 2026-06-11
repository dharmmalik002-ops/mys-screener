import { useMemo, useState } from "react";
import type { ChangeEvent } from "react";

import type { MomentumBurstScanRequest, ScanMatch } from "../lib/api";
import { Panel } from "./Panel";

// ---------------------------------------------------------------------------
// Settings panel — every threshold from the spec is editable here.
// ---------------------------------------------------------------------------

type MomentumBurstScannerPanelProps = {
  filters: MomentumBurstScanRequest;
  onFiltersChange: (filters: MomentumBurstScanRequest) => void;
  onApply: () => void;
  onReset: () => void;
};

function numField(
  filters: MomentumBurstScanRequest,
  onFiltersChange: (filters: MomentumBurstScanRequest) => void,
  field: keyof MomentumBurstScanRequest,
  minValue?: number,
) {
  return (event: ChangeEvent<HTMLInputElement>) => {
    const next = Number(event.target.value);
    onFiltersChange({
      ...filters,
      [field]: Number.isFinite(next) ? (minValue !== undefined ? Math.max(minValue, next) : next) : filters[field],
    });
  };
}

function toggleField(
  filters: MomentumBurstScanRequest,
  onFiltersChange: (filters: MomentumBurstScanRequest) => void,
  field: keyof MomentumBurstScanRequest,
) {
  return (event: ChangeEvent<HTMLInputElement>) => {
    onFiltersChange({ ...filters, [field]: event.target.checked });
  };
}

export function MomentumBurstScannerPanel({
  filters,
  onFiltersChange,
  onApply,
  onReset,
}: MomentumBurstScannerPanelProps) {
  const f = filters;
  const set = onFiltersChange;
  return (
    <Panel
      title="Momentum Burst"
      subtitle="Fresh explosive legs (Burst) plus the buyable rest near the 10/21 EMA. Moving averages, price action, volume and RS only — no oscillators."
      actions={
        <div className="custom-panel-actions">
          <button type="button" className="nav-button ghost" onClick={onReset}>
            Reset
          </button>
          <button type="button" className="nav-button primary" onClick={onApply}>
            Apply Filters
          </button>
        </div>
      }
      className="gap-up-panel"
    >
      <div className="scanner-settings-note">
        <strong>Universe &amp; trend</strong>
        <span>Price &gt; ₹{f.min_price}, 20-day turnover &gt; ₹{f.min_turnover_crore} Cr, close &gt; 50 SMA &gt; 200 SMA, RS ≥ {f.min_rs_rating}.</span>
      </div>

      <div className="scanner-section-grid near-pivot-grid">
        <label className="scanner-field">
          <span>Min Price (₹)</span>
          <input type="number" min="0" step="1" value={f.min_price} onChange={numField(f, set, "min_price", 0)} />
          <small>Default 50</small>
        </label>
        <label className="scanner-field">
          <span>Min 20D Turnover (₹ Cr)</span>
          <input type="number" min="0" step="0.5" value={f.min_turnover_crore} onChange={numField(f, set, "min_turnover_crore", 0)} />
          <small>Default 5</small>
        </label>
        <label className="scanner-field">
          <span>Min RS Rating</span>
          <input type="number" min="1" max="99" step="1" value={f.min_rs_rating} onChange={numField(f, set, "min_rs_rating", 1)} />
          <small>Percentile vs scanned universe. Default 70</small>
        </label>
        <label className="scanner-field">
          <span>Result Limit</span>
          <input type="number" min="1" max="5000" step="1" value={f.limit} onChange={numField(f, set, "limit", 1)} />
          <small>Default 1500</small>
        </label>
      </div>

      <div className="scanner-checkbox-line">
        <label>
          <input type="checkbox" checked={f.include_ema_setups} onChange={toggleField(f, set, "include_ema_setups")} />
          <span>Include 10/21 EMA setups (primary list)</span>
        </label>
        <label>
          <input type="checkbox" checked={f.include_fresh_bursts} onChange={toggleField(f, set, "include_fresh_bursts")} />
          <span>Include fresh bursts</span>
        </label>
      </div>

      <div className="scanner-settings-note">
        <strong>Type A — Fresh Momentum Burst</strong>
      </div>
      <div className="scanner-section-grid near-pivot-grid">
        <label className="scanner-field">
          <span>Min Burst Gain %</span>
          <input type="number" min="1" step="0.5" value={f.burst_min_gain_pct} onChange={numField(f, set, "burst_min_gain_pct", 1)} />
          <small>Default 15</small>
        </label>
        <label className="scanner-field">
          <span>Burst Window (min days)</span>
          <input type="number" min="1" step="1" value={f.burst_window_min} onChange={numField(f, set, "burst_window_min", 1)} />
          <small>Rolling window start. Default 3</small>
        </label>
        <label className="scanner-field">
          <span>Burst Window (max days)</span>
          <input type="number" min="1" step="1" value={f.burst_window_max} onChange={numField(f, set, "burst_window_max", 1)} />
          <small>Default 10</small>
        </label>
        <label className="scanner-field">
          <span>Burst Ends Within (sessions)</span>
          <input type="number" min="1" step="1" value={f.burst_recency_sessions} onChange={numField(f, set, "burst_recency_sessions", 1)} />
          <small>Default 5</small>
        </label>
        <label className="scanner-field">
          <span>Min Volume × 50D Avg</span>
          <input type="number" min="0.5" step="0.1" value={f.burst_min_volume_ratio} onChange={numField(f, set, "burst_min_volume_ratio", 0.5)} />
          <small>A day in the move with this RVOL. Default 1.5</small>
        </label>
      </div>

      <div className="scanner-settings-note">
        <strong>Type B — Consolidation near the 10/21 EMA</strong>
      </div>
      <div className="scanner-section-grid near-pivot-grid">
        <label className="scanner-field">
          <span>Min Prior Move %</span>
          <input type="number" min="1" step="0.5" value={f.setup_min_move_pct} onChange={numField(f, set, "setup_min_move_pct", 1)} />
          <small>Default 20</small>
        </label>
        <label className="scanner-field">
          <span>Move Window (min days)</span>
          <input type="number" min="1" step="1" value={f.setup_move_window_min} onChange={numField(f, set, "setup_move_window_min", 1)} />
          <small>Default 5</small>
        </label>
        <label className="scanner-field">
          <span>Move Window (max days)</span>
          <input type="number" min="1" step="1" value={f.setup_move_window_max} onChange={numField(f, set, "setup_move_window_max", 1)} />
          <small>Default 15</small>
        </label>
        <label className="scanner-field">
          <span>Move Lookback (sessions)</span>
          <input type="number" min="2" step="1" value={f.setup_move_lookback_sessions} onChange={numField(f, set, "setup_move_lookback_sessions", 2)} />
          <small>Default 30</small>
        </label>
        <label className="scanner-field">
          <span>Consolidation Min Days</span>
          <input type="number" min="2" step="1" value={f.consolidation_min_days} onChange={numField(f, set, "consolidation_min_days", 2)} />
          <small>Default 3</small>
        </label>
        <label className="scanner-field">
          <span>Consolidation Max Days</span>
          <input type="number" min="2" step="1" value={f.consolidation_max_days} onChange={numField(f, set, "consolidation_max_days", 2)} />
          <small>Default 15</small>
        </label>
        <label className="scanner-field">
          <span>Consolidation Range % (max)</span>
          <input type="number" min="0.5" step="0.5" value={f.consolidation_max_range_pct} onChange={numField(f, set, "consolidation_max_range_pct", 0.5)} />
          <small>High-to-low of the rest. Default 10</small>
        </label>
        <label className="scanner-field">
          <span>EMA Surf Distance % (±)</span>
          <input type="number" min="0.5" step="0.5" value={f.ema_surf_distance_pct} onChange={numField(f, set, "ema_surf_distance_pct", 0.5)} />
          <small>Closes within this of the EMA. Default 4</small>
        </label>
        <label className="scanner-field">
          <span>Max Giveback % (10 EMA)</span>
          <input type="number" min="1" step="1" value={f.max_giveback_10ema_pct} onChange={numField(f, set, "max_giveback_10ema_pct", 1)} />
          <small>&lt; one-third. Default 33.33</small>
        </label>
        <label className="scanner-field">
          <span>Max Giveback % (21 EMA)</span>
          <input type="number" min="1" step="1" value={f.max_giveback_21ema_pct} onChange={numField(f, set, "max_giveback_21ema_pct", 1)} />
          <small>&lt; half. Default 50</small>
        </label>
        <label className="scanner-field">
          <span>Volume Dry-up Ratio (max)</span>
          <input type="number" min="0.1" step="0.05" value={f.volume_dryup_ratio} onChange={numField(f, set, "volume_dryup_ratio", 0.1)} />
          <small>Rest vol &lt; this × burst-leg vol. Default 0.7</small>
        </label>
      </div>
    </Panel>
  );
}

// ---------------------------------------------------------------------------
// Results — a trade plan per stock, sortable, grouped by tag.
// ---------------------------------------------------------------------------

type MomentumBurstResultsProps = {
  items: ScanMatch[];
  loading: boolean;
  onPickSymbol: (symbol: string) => void;
  selectedSymbol?: string | null;
};

type SortKey =
  | "default"
  | "rs"
  | "close"
  | "burst"
  | "burst_days"
  | "cons_days"
  | "range"
  | "dist10"
  | "dist21"
  | "dryup"
  | "risk";

const TAG_RANK: Record<string, number> = { "10 EMA Setup": 0, "21 EMA Setup": 1, Burst: 2 };

function tagColor(tag: string): string {
  if (tag === "10 EMA Setup") return "#37b24d";
  if (tag === "21 EMA Setup") return "#74b816";
  return "#1c7ed6";
}

function fmt(n: number | null | undefined, digits = 2, suffix = ""): string {
  if (n === null || n === undefined || !Number.isFinite(n)) return "—";
  return `${n.toFixed(digits)}${suffix}`;
}

export function MomentumBurstResults({ items, loading, onPickSymbol, selectedSymbol }: MomentumBurstResultsProps) {
  const [sortKey, setSortKey] = useState<SortKey>("default");
  const [sortDir, setSortDir] = useState<"asc" | "desc">("desc");

  const rows = useMemo(() => items.filter((it) => it.momentum_burst), [items]);

  const sorted = useMemo(() => {
    const copy = [...rows];
    const dir = sortDir === "asc" ? 1 : -1;
    const plan = (m: ScanMatch) => m.momentum_burst!;
    copy.sort((a, b) => {
      if (sortKey === "default") {
        // 10 EMA setups, then 21 EMA setups, then Bursts; RS desc within each.
        const ta = TAG_RANK[plan(a).tag] ?? 9;
        const tb = TAG_RANK[plan(b).tag] ?? 9;
        if (ta !== tb) return ta - tb;
        return plan(b).rs_rating - plan(a).rs_rating;
      }
      const pick = (m: ScanMatch): number => {
        const p = plan(m);
        switch (sortKey) {
          case "rs": return p.rs_rating;
          case "close": return m.last_price;
          case "burst": return p.burst_pct;
          case "burst_days": return p.burst_days;
          case "cons_days": return p.consolidation_days ?? -1;
          case "range": return p.consolidation_range_pct ?? -1;
          case "dist10": return p.dist_from_10ema_pct ?? 0;
          case "dist21": return p.dist_from_21ema_pct ?? 0;
          case "dryup": return p.volume_dryup_ratio ?? 999;
          case "risk": return p.risk_pct ?? 999;
          default: return 0;
        }
      };
      return (pick(a) - pick(b)) * dir;
    });
    return copy;
  }, [rows, sortKey, sortDir]);

  const onSort = (key: SortKey) => {
    if (key === sortKey) {
      setSortDir((d) => (d === "asc" ? "desc" : "asc"));
    } else {
      setSortKey(key);
      setSortDir(key === "default" ? "desc" : "desc");
    }
  };

  const cols: Array<{ key: SortKey; label: string; sortable: boolean }> = [
    { key: "default", label: "Stock / Tag", sortable: true },
    { key: "rs", label: "RS", sortable: true },
    { key: "close", label: "Close", sortable: true },
    { key: "burst", label: "Burst %", sortable: true },
    { key: "burst_days", label: "Days", sortable: true },
    { key: "cons_days", label: "Cons.", sortable: true },
    { key: "range", label: "Range %", sortable: true },
    { key: "dist10", label: "Δ10EMA", sortable: true },
    { key: "dist21", label: "Δ21EMA", sortable: true },
    { key: "dryup", label: "Vol dry", sortable: true },
    { key: "default", label: "Entry", sortable: false },
    { key: "risk", label: "Stop (risk)", sortable: true },
    { key: "default", label: "2R / 3R", sortable: false },
  ];

  if (loading && rows.length === 0) {
    return <div className="empty-state">Scanning for momentum bursts…</div>;
  }
  if (rows.length === 0) {
    return <div className="empty-state">No stocks match the momentum burst filters. Loosen the thresholds and re-apply.</div>;
  }

  const th: React.CSSProperties = {
    textAlign: "right",
    padding: "6px 8px",
    fontSize: 11,
    color: "#9aa4b2",
    fontWeight: 600,
    whiteSpace: "nowrap",
    cursor: "pointer",
    userSelect: "none",
    borderBottom: "1px solid rgba(255,255,255,0.08)",
  };
  const td: React.CSSProperties = {
    textAlign: "right",
    padding: "7px 8px",
    fontSize: 12,
    whiteSpace: "nowrap",
    borderBottom: "1px solid rgba(255,255,255,0.05)",
  };

  return (
    <div className="momentum-burst-results" style={{ overflowX: "auto" }}>
      <table style={{ width: "100%", borderCollapse: "collapse" }}>
        <thead>
          <tr>
            {cols.map((c, i) => (
              <th
                key={`${c.label}-${i}`}
                style={{ ...th, textAlign: i === 0 ? "left" : "right" }}
                onClick={c.sortable ? () => onSort(c.key) : undefined}
                title={c.sortable ? "Click to sort" : undefined}
              >
                {c.label}
                {c.sortable && sortKey === c.key ? (sortDir === "asc" ? " ▲" : " ▼") : ""}
              </th>
            ))}
          </tr>
        </thead>
        <tbody>
          {sorted.map((m) => {
            const p = m.momentum_burst!;
            const isSel = selectedSymbol === m.symbol;
            const isSetup = p.tag !== "Burst";
            return (
              <tr
                key={m.symbol}
                onClick={() => onPickSymbol(m.symbol)}
                style={{ cursor: "pointer", background: isSel ? "rgba(255,255,255,0.06)" : undefined }}
              >
                <td style={{ ...td, textAlign: "left" }}>
                  <div style={{ display: "flex", flexDirection: "column", gap: 2 }}>
                    <strong style={{ fontSize: 13 }}>{m.symbol}</strong>
                    <span
                      style={{
                        fontSize: 10,
                        fontWeight: 700,
                        color: tagColor(p.tag),
                        textTransform: "uppercase",
                        letterSpacing: 0.3,
                      }}
                    >
                      {p.tag}
                    </span>
                  </div>
                </td>
                <td style={td}>{p.rs_rating}</td>
                <td style={td}>{fmt(m.last_price)}</td>
                <td style={{ ...td, color: "#37b24d" }}>+{fmt(p.burst_pct, 1)}%</td>
                <td style={td}>{p.burst_days}</td>
                <td style={td}>{p.consolidation_days ?? "—"}</td>
                <td style={td}>{fmt(p.consolidation_range_pct, 1, "%")}</td>
                <td style={td}>{p.dist_from_10ema_pct === null || p.dist_from_10ema_pct === undefined ? "—" : `${p.dist_from_10ema_pct > 0 ? "+" : ""}${fmt(p.dist_from_10ema_pct, 1)}%`}</td>
                <td style={td}>{p.dist_from_21ema_pct === null || p.dist_from_21ema_pct === undefined ? "—" : `${p.dist_from_21ema_pct > 0 ? "+" : ""}${fmt(p.dist_from_21ema_pct, 1)}%`}</td>
                <td style={td}>{p.volume_dryup_ratio === null || p.volume_dryup_ratio === undefined ? "—" : `${fmt(p.volume_dryup_ratio, 2)}×`}</td>
                <td style={td}>{isSetup ? fmt(p.entry) : "—"}</td>
                <td style={td}>
                  {isSetup ? (
                    <span>
                      {fmt(p.stop)}
                      {p.risk_pct !== null && p.risk_pct !== undefined ? (
                        <span style={{ color: "#fa5252" }}> ({fmt(p.risk_pct, 1)}%)</span>
                      ) : null}
                    </span>
                  ) : (
                    "—"
                  )}
                </td>
                <td style={td}>{isSetup ? `${fmt(p.target_2r)} / ${fmt(p.target_3r)}` : "—"}</td>
              </tr>
            );
          })}
        </tbody>
      </table>
    </div>
  );
}
