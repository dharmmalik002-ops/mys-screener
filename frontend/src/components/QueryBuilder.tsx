import { useMemo, useState } from "react";
import { Plus, X, Layers, Filter as FilterIcon, Sparkles } from "lucide-react";

import type { CustomScanRequest } from "../lib/api";

import "./QueryBuilder.css";

type QueryBuilderProps = {
  filters: CustomScanRequest;
  defaults: CustomScanRequest;
  onFiltersChange: (filters: CustomScanRequest) => void;
  /** Toggle / scroll to the underlying filter form. */
  onAddFilter: () => void;
};

type ActiveChip = {
  key: keyof CustomScanRequest;
  label: string;
  value: string;
  /** Default value to reset to when the chip is removed. */
  defaultValue: CustomScanRequest[keyof CustomScanRequest];
};

type GroupOp = "AND" | "OR";

/* ---------- Field labels & formatters ---------- */
type FieldDef = { label: string; format?: (v: unknown) => string };

const FIELD_LABELS: Partial<Record<keyof CustomScanRequest, FieldDef>> = {
  min_price: { label: "Price ≥", format: (v) => `₹${v}` },
  max_price: { label: "Price ≤", format: (v) => `₹${v}` },
  min_market_cap_crore: { label: "Mkt Cap ≥", format: (v) => `${v} Cr` },
  max_market_cap_crore: { label: "Mkt Cap ≤", format: (v) => `${v} Cr` },
  min_change_pct: { label: "Day Δ ≥", format: (v) => `${v}%` },
  max_change_pct: { label: "Day Δ ≤", format: (v) => `${v}%` },
  min_relative_volume: { label: "Rel Vol ≥", format: (v) => `${v}×` },
  min_rs_rating: { label: "RS Rating ≥" },
  max_rs_rating: { label: "RS Rating ≤" },
  min_three_month_rs: { label: "3M RS ≥" },
  min_nifty_outperformance: { label: "RS vs Nifty ≥", format: (v) => `${v}%` },
  min_sector_outperformance: { label: "RS vs Sector ≥", format: (v) => `${v}%` },
  min_avg_rupee_volume_30d_crore: { label: "30D Avg ₹Vol ≥", format: (v) => `${v} Cr` },
  min_avg_rupee_turnover_20d_crore: { label: "20D Turnover ≥", format: (v) => `${v} Cr` },
  min_pct_from_52w_high: { label: "% from 52WH ≥", format: (v) => `${v}%` },
  max_pct_from_52w_high: { label: "% from 52WH ≤", format: (v) => `${v}%` },
  min_pct_from_52w_low: { label: "% from 52WL ≥", format: (v) => `${v}%` },
  max_pct_from_52w_low: { label: "% from 52WL ≤", format: (v) => `${v}%` },
  min_pct_from_ath: { label: "% from ATH ≥", format: (v) => `${v}%` },
  max_pct_from_ath: { label: "% from ATH ≤", format: (v) => `${v}%` },
  min_gap_pct: { label: "Gap ≥", format: (v) => `${v}%` },
  max_gap_pct: { label: "Gap ≤", format: (v) => `${v}%` },
  min_day_range_pct: { label: "Day Range ≥", format: (v) => `${v}%` },
  max_day_range_pct: { label: "Day Range ≤", format: (v) => `${v}%` },
  min_return_pct: { label: "Return ≥", format: (v) => `${v}%` },
  max_return_pct: { label: "Return ≤", format: (v) => `${v}%` },
  min_trend_strength: { label: "Trend ≥" },
  max_pullback_depth_pct: { label: "Pullback ≤", format: (v) => `${v}%` },
  near_high_period: { label: "Near High", format: (v) => String(v) },
  near_high_max_distance_pct: { label: "Max Dist from High", format: (v) => `${v}%` },
  pattern: { label: "Pattern", format: (v) => String(v) },
  return_period: { label: "Return Period", format: (v) => String(v) },
  price_vs_ma_mode: { label: "Price vs MA", format: (v) => String(v) },
  price_vs_ma_key: { label: "MA", format: (v) => String(v).toUpperCase() },
  above_ema20: { label: "Above 20 EMA", format: () => "✓" },
  above_ema50: { label: "Above 50 EMA", format: () => "✓" },
  above_ema200: { label: "Above 200 SMA", format: () => "✓" },
  require_bullish_ma_order: { label: "Bullish MA Stack", format: () => "✓" },
  require_bearish_ma_order: { label: "Bearish MA Stack", format: () => "✓" },
  listing_date_from: { label: "Listed ≥" },
  listing_date_to: { label: "Listed ≤" },
};

function isDefault(value: unknown, defaultValue: unknown): boolean {
  if (value === defaultValue) return true;
  if (value === null && (defaultValue === null || defaultValue === undefined)) return true;
  if (value === undefined && defaultValue === null) return true;
  return false;
}

export function QueryBuilder({
  filters,
  defaults,
  onFiltersChange,
  onAddFilter,
}: QueryBuilderProps) {
  const [groupOp, setGroupOp] = useState<GroupOp>("AND");
  const [extraGroups, setExtraGroups] = useState<number>(0);

  const chips: ActiveChip[] = useMemo(() => {
    const out: ActiveChip[] = [];
    (Object.keys(FIELD_LABELS) as Array<keyof CustomScanRequest>).forEach((key) => {
      const def = FIELD_LABELS[key];
      if (!def) return;
      const cur = filters[key];
      const dflt = defaults[key];
      // Skip "any" / empty / falsy when default is the same kind of falsy
      if (cur === "any" && dflt === "any") return;
      if (typeof cur === "boolean" && cur === false) return;
      if (cur === null || cur === undefined || cur === "") return;
      if (isDefault(cur, dflt)) return;

      const value = def.format ? def.format(cur) : String(cur);
      out.push({ key, label: def.label, value, defaultValue: dflt as CustomScanRequest[keyof CustomScanRequest] });
    });
    return out;
  }, [filters, defaults]);

  const removeChip = (chip: ActiveChip) => {
    onFiltersChange({ ...filters, [chip.key]: chip.defaultValue });
  };

  const clearAll = () => {
    onFiltersChange(defaults);
    setExtraGroups(0);
  };

  return (
    <section className="qb-root">
      <div className="qb-head">
        <span className="qb-head-icon" aria-hidden>
          <Sparkles size={13} strokeWidth={2.4} />
        </span>
        <strong>Query Builder</strong>
        <span className="qb-head-meta">
          {chips.length === 0
            ? "No filters active"
            : `${chips.length} filter${chips.length === 1 ? "" : "s"} stacked`}
        </span>
        {chips.length > 0 ? (
          <button type="button" className="qb-clear" onClick={clearAll}>
            Clear all
          </button>
        ) : null}
      </div>

      <div className="qb-canvas">
        <div className="qb-group">
          <div className="qb-group-head">
            <button
              type="button"
              className="qb-op-toggle"
              onClick={() => setGroupOp((cur) => (cur === "AND" ? "OR" : "AND"))}
              title="Toggle group operator"
            >
              {groupOp}
            </button>
            <span className="qb-group-title">Group 1 — match {groupOp === "AND" ? "all" : "any"} of:</span>
          </div>

          <div className="qb-chips">
            {chips.length === 0 ? (
              <div className="qb-empty">
                <FilterIcon size={13} />
                <span>Click "Add Filter" or open Settings to start stacking criteria.</span>
              </div>
            ) : (
              chips.map((chip) => (
                <span key={String(chip.key)} className="qb-chip">
                  <span className="qb-chip-label">{chip.label}</span>
                  <span className="qb-chip-value">{chip.value}</span>
                  <button
                    type="button"
                    className="qb-chip-remove"
                    onClick={() => removeChip(chip)}
                    aria-label={`Remove ${chip.label}`}
                  >
                    <X size={11} strokeWidth={2.6} />
                  </button>
                </span>
              ))
            )}
          </div>

          <div className="qb-actions">
            <button type="button" className="qb-action qb-action-primary" onClick={onAddFilter}>
              <Plus size={13} strokeWidth={2.4} />
              <span>Add Filter</span>
            </button>
            <button
              type="button"
              className="qb-action"
              onClick={() => setExtraGroups((c) => c + 1)}
            >
              <Layers size={13} strokeWidth={2.2} />
              <span>Add Group</span>
            </button>
          </div>
        </div>

        {Array.from({ length: extraGroups }).map((_, i) => (
          <div key={`extra-${i}`} className="qb-group qb-group-extra">
            <div className="qb-group-head">
              <button
                type="button"
                className="qb-op-toggle qb-op-or"
                title="Sub-groups currently OR with main group"
              >
                OR
              </button>
              <span className="qb-group-title">Group {i + 2} — placeholder</span>
              <button
                type="button"
                className="qb-group-remove"
                onClick={() => setExtraGroups((c) => Math.max(0, c - 1))}
                aria-label="Remove group"
              >
                <X size={12} strokeWidth={2.4} />
              </button>
            </div>
            <div className="qb-empty qb-empty-sub">
              <FilterIcon size={13} />
              <span>Empty group · Add filters via Settings to populate.</span>
            </div>
          </div>
        ))}
      </div>
    </section>
  );
}
