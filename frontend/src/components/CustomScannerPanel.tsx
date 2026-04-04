import { useState, type ChangeEvent, type ReactNode } from "react";

import type {
  CustomScanRequest,
  CustomSortBy,
  MaKey,
  NearHighPeriod,
  ReturnPeriod,
  ScanDescriptor,
} from "../lib/api";
import { Panel } from "./Panel";

type CustomScannerPanelProps = {
  filters: CustomScanRequest;
  patternOptions: ScanDescriptor[];
  onFiltersChange: (filters: CustomScanRequest) => void;
  onApply: () => void;
  onReset: () => void;
};

type ScannerTab = "technicals" | "fundamentals" | "misc";

type NumericField = {
  [K in keyof CustomScanRequest]: CustomScanRequest[K] extends number | null ? K : never;
}[keyof CustomScanRequest];

const TABS: Array<{ key: ScannerTab; label: string }> = [
  { key: "technicals", label: "Technicals" },
  { key: "fundamentals", label: "Fundamentals" },
  { key: "misc", label: "Miscellaneous" },
];

const MA_OPTIONS: Array<{ value: MaKey; label: string }> = [
  { value: "ema10", label: "10 EMA" },
  { value: "ema20", label: "20 EMA" },
  { value: "ema50", label: "50 EMA" },
  { value: "ema200", label: "200 Day SMA" },
];

const RETURN_PERIODS: Array<{ value: ReturnPeriod; label: string }> = [
  { value: "1D", label: "1 Day" },
  { value: "1W", label: "1 Week" },
  { value: "1M", label: "1 Month" },
  { value: "3M", label: "3 Month" },
  { value: "6M", label: "6 Month" },
  { value: "1Y", label: "1 Year" },
];

const NEAR_HIGH_OPTIONS: Array<{ value: NearHighPeriod; label: string }> = [
  { value: "1M", label: "1 Month High" },
  { value: "3M", label: "3 Month High" },
  { value: "6M", label: "6 Month High" },
  { value: "52W", label: "52 Week High" },
  { value: "ATH", label: "All-Time High" },
];

const SORT_OPTIONS: Array<{ value: CustomSortBy; label: string }> = [
  { value: "pattern", label: "Pattern Score" },
  { value: "rs_rating", label: "RS Rating" },
  { value: "listing_date", label: "Listing Date" },
  { value: "three_month_rs", label: "3M RS" },
  { value: "relative_strength", label: "RS vs Benchmark" },
  { value: "relative_volume", label: "Relative Volume" },
  { value: "avg_rupee_volume", label: "Traded Value" },
  { value: "stock_return_12m", label: "12M Return" },
  { value: "stock_return_60d", label: "3M Return" },
  { value: "stock_return_20d", label: "1M Return" },
  { value: "change_pct", label: "Day Change" },
  { value: "price", label: "Price" },
  { value: "market_cap", label: "Market Cap" },
];

function toInputValue(value: number | null) {
  return value === null ? "" : String(value);
}

function FilterField({
  label,
  children,
  hint,
}: {
  label: string;
  children: ReactNode;
  hint?: string;
}) {
  return (
    <label className="scanner-field">
      <span>{label}</span>
      {children}
      {hint ? <small>{hint}</small> : null}
    </label>
  );
}

function RangeInputs({
  minValue,
  maxValue,
  minLabel = "Min",
  maxLabel = "Max",
  minStep = "0.1",
  maxStep = "0.1",
  onMinChange,
  onMaxChange,
}: {
  minValue: number | null;
  maxValue: number | null;
  minLabel?: string;
  maxLabel?: string;
  minStep?: string;
  maxStep?: string;
  onMinChange: (event: ChangeEvent<HTMLInputElement>) => void;
  onMaxChange: (event: ChangeEvent<HTMLInputElement>) => void;
}) {
  return (
    <div className="range-inputs">
      <input type="number" step={minStep} placeholder={minLabel} value={toInputValue(minValue)} onChange={onMinChange} />
      <span>-</span>
      <input type="number" step={maxStep} placeholder={maxLabel} value={toInputValue(maxValue)} onChange={onMaxChange} />
    </div>
  );
}

export function CustomScannerPanel({
  filters,
  patternOptions,
  onFiltersChange,
  onApply,
  onReset,
}: CustomScannerPanelProps) {
  const [activeTab, setActiveTab] = useState<ScannerTab>("technicals");

  const updateNumeric = (field: NumericField) => (event: ChangeEvent<HTMLInputElement>) => {
    const value = event.target.value.trim();
    onFiltersChange({
      ...filters,
      [field]: value === "" ? null : Number(value),
    });
  };

  const updateDate =
    (field: keyof Pick<CustomScanRequest, "listing_date_from" | "listing_date_to">) =>
    (event: ChangeEvent<HTMLInputElement>) => {
      const value = event.target.value.trim();
      onFiltersChange({
        ...filters,
        [field]: value === "" ? null : value,
      });
    };

  const updateBoolean =
    (field: keyof Pick<CustomScanRequest, "above_ema20" | "above_ema50" | "above_ema200" | "require_bullish_ma_order" | "require_bearish_ma_order">) =>
    (event: ChangeEvent<HTMLInputElement>) => {
      onFiltersChange({
        ...filters,
        [field]: event.target.checked,
      });
    };

  const updateSelect =
    <K extends keyof Pick<CustomScanRequest, "pattern" | "sort_by" | "sort_order" | "price_vs_ma_mode" | "price_vs_ma_key" | "price_to_ma_key" | "return_period">>(
      field: K,
    ) =>
    (event: ChangeEvent<HTMLSelectElement>) => {
      onFiltersChange({
        ...filters,
        [field]: event.target.value as CustomScanRequest[K],
      });
    };

  return (
    <Panel
      title="Custom Scanner"
      subtitle="Filter the full universe by price, RS, volume, momentum, moving averages, and breakout context."
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
      className="custom-panel"
    >
      <div className="scanner-tab-row">
        {TABS.map((tab) => (
          <button
            key={tab.key}
            type="button"
            className={tab.key === activeTab ? "scanner-tab active" : "scanner-tab"}
            onClick={() => setActiveTab(tab.key)}
          >
            {tab.label}
          </button>
        ))}
      </div>

      <div className="scanner-hero-grid">
        <FilterField label="Pattern">
          <select value={filters.pattern} onChange={updateSelect("pattern")}>
            <option value="any">Any Pattern</option>
            {patternOptions.map((option) => (
              <option key={option.id} value={option.id}>
                {option.name}
              </option>
            ))}
          </select>
        </FilterField>

        <FilterField label="Sort By">
          <select value={filters.sort_by} onChange={updateSelect("sort_by")}>
            {SORT_OPTIONS.map((option) => (
              <option key={option.value} value={option.value}>
                {option.label}
              </option>
            ))}
          </select>
        </FilterField>

        <FilterField label="Order">
          <select value={filters.sort_order} onChange={updateSelect("sort_order")}>
            <option value="desc">High to Low</option>
            <option value="asc">Low to High</option>
          </select>
        </FilterField>

        <FilterField label="Result Limit" hint="Set this high to scan the whole universe.">
          <input
            type="number"
            min="1"
            max="5000"
            value={String(filters.limit)}
            onChange={(event) =>
              onFiltersChange({
                ...filters,
                limit: Math.max(1, Math.min(5000, Number(event.target.value) || 1500)),
              })
            }
          />
        </FilterField>
      </div>

      {activeTab === "technicals" ? (
        <div className="scanner-section-grid">
          <FilterField label="Return Range">
            <div className="stacked-field">
              <select value={filters.return_period} onChange={updateSelect("return_period")}>
                {RETURN_PERIODS.map((option) => (
                  <option key={option.value} value={option.value}>
                    {option.label}
                  </option>
                ))}
              </select>
              <RangeInputs
                minValue={filters.min_return_pct}
                maxValue={filters.max_return_pct}
                onMinChange={updateNumeric("min_return_pct")}
                onMaxChange={updateNumeric("max_return_pct")}
              />
            </div>
          </FilterField>

          <FilterField label="Stock Price">
            <RangeInputs
              minValue={filters.min_price}
              maxValue={filters.max_price}
              minStep="0.01"
              maxStep="0.01"
              onMinChange={updateNumeric("min_price")}
              onMaxChange={updateNumeric("max_price")}
            />
          </FilterField>

          <FilterField label="Overall RS Rating">
            <RangeInputs
              minValue={filters.min_rs_rating}
              maxValue={filters.max_rs_rating}
              minStep="1"
              maxStep="1"
              onMinChange={updateNumeric("min_rs_rating")}
              onMaxChange={updateNumeric("max_rs_rating")}
            />
          </FilterField>

          <FilterField label="3 Month RS">
            <input type="number" step="0.1" value={toInputValue(filters.min_three_month_rs)} onChange={updateNumeric("min_three_month_rs")} />
          </FilterField>

          <FilterField label="RS vs Benchmark">
            <input
              type="number"
              step="0.1"
              value={toInputValue(filters.min_nifty_outperformance)}
              onChange={updateNumeric("min_nifty_outperformance")}
            />
          </FilterField>

          <FilterField label="RS vs Sector">
            <input
              type="number"
              step="0.1"
              value={toInputValue(filters.min_sector_outperformance)}
              onChange={updateNumeric("min_sector_outperformance")}
            />
          </FilterField>

          <FilterField label="Near New Highs">
            <div className="stacked-field">
              <select
                value={filters.near_high_period ?? ""}
                onChange={(event) =>
                  onFiltersChange({
                    ...filters,
                    near_high_period: event.target.value ? (event.target.value as NearHighPeriod) : null,
                  })
                }
              >
                <option value="">Any</option>
                {NEAR_HIGH_OPTIONS.map((option) => (
                  <option key={option.value} value={option.value}>
                    {option.label}
                  </option>
                ))}
              </select>
              <input
                type="number"
                step="0.1"
                placeholder="Max distance %"
                value={toInputValue(filters.near_high_max_distance_pct)}
                onChange={updateNumeric("near_high_max_distance_pct")}
              />
            </div>
          </FilterField>

          <FilterField label="% From 52W High">
            <RangeInputs
              minValue={filters.min_pct_from_52w_high}
              maxValue={filters.max_pct_from_52w_high}
              onMinChange={updateNumeric("min_pct_from_52w_high")}
              onMaxChange={updateNumeric("max_pct_from_52w_high")}
            />
          </FilterField>

          <FilterField label="% From ATH">
            <RangeInputs
              minValue={filters.min_pct_from_ath}
              maxValue={filters.max_pct_from_ath}
              onMinChange={updateNumeric("min_pct_from_ath")}
              onMaxChange={updateNumeric("max_pct_from_ath")}
            />
          </FilterField>

          <FilterField label="% From 52W Low">
            <RangeInputs
              minValue={filters.min_pct_from_52w_low}
              maxValue={filters.max_pct_from_52w_low}
              onMinChange={updateNumeric("min_pct_from_52w_low")}
              onMaxChange={updateNumeric("max_pct_from_52w_low")}
            />
          </FilterField>

          <FilterField label="Relative Volume">
            <input type="number" step="0.1" value={toInputValue(filters.min_relative_volume)} onChange={updateNumeric("min_relative_volume")} />
          </FilterField>

          <FilterField label="Gap %">
            <RangeInputs
              minValue={filters.min_gap_pct}
              maxValue={filters.max_gap_pct}
              onMinChange={updateNumeric("min_gap_pct")}
              onMaxChange={updateNumeric("max_gap_pct")}
            />
          </FilterField>

          <FilterField label="Day Range %">
            <RangeInputs
              minValue={filters.min_day_range_pct}
              maxValue={filters.max_day_range_pct}
              onMinChange={updateNumeric("min_day_range_pct")}
              onMaxChange={updateNumeric("max_day_range_pct")}
            />
          </FilterField>

          <FilterField label="Price vs Moving Average">
            <div className="split-row">
              <select value={filters.price_vs_ma_mode} onChange={updateSelect("price_vs_ma_mode")}>
                <option value="any">Any</option>
                <option value="above">Above</option>
                <option value="below">Below</option>
              </select>
              <select value={filters.price_vs_ma_key} onChange={updateSelect("price_vs_ma_key")}>
                {MA_OPTIONS.map((option) => (
                  <option key={option.value} value={option.value}>
                    {option.label}
                  </option>
                ))}
              </select>
            </div>
          </FilterField>

          <FilterField label="Price / MA Ratio">
            <div className="stacked-field">
              <select value={filters.price_to_ma_key} onChange={updateSelect("price_to_ma_key")}>
                {MA_OPTIONS.map((option) => (
                  <option key={option.value} value={option.value}>
                    {option.label}
                  </option>
                ))}
              </select>
              <RangeInputs
                minValue={filters.min_price_to_ma_ratio}
                maxValue={filters.max_price_to_ma_ratio}
                minStep="0.01"
                maxStep="0.01"
                onMinChange={updateNumeric("min_price_to_ma_ratio")}
                onMaxChange={updateNumeric("max_price_to_ma_ratio")}
              />
            </div>
          </FilterField>

          <FilterField label="Moving Average Alignment">
            <div className="scanner-toggle-grid">
              <label className="scanner-toggle">
                <input type="checkbox" checked={filters.require_bullish_ma_order} onChange={updateBoolean("require_bullish_ma_order")} />
                <span>20 EMA &gt;= 50 EMA &gt;= 200 Day SMA</span>
              </label>
              <label className="scanner-toggle">
                <input type="checkbox" checked={filters.require_bearish_ma_order} onChange={updateBoolean("require_bearish_ma_order")} />
                <span>20 EMA &lt;= 50 EMA &lt;= 200 Day SMA</span>
              </label>
            </div>
          </FilterField>

          <FilterField label="Quick MA Checks">
            <div className="scanner-toggle-grid compact">
              <label className="scanner-toggle">
                <input type="checkbox" checked={filters.above_ema20} onChange={updateBoolean("above_ema20")} />
                <span>Above 20 EMA</span>
              </label>
              <label className="scanner-toggle">
                <input type="checkbox" checked={filters.above_ema50} onChange={updateBoolean("above_ema50")} />
                <span>Above 50 EMA</span>
              </label>
              <label className="scanner-toggle">
                <input type="checkbox" checked={filters.above_ema200} onChange={updateBoolean("above_ema200")} />
                <span>Above 200 Day SMA</span>
              </label>
            </div>
          </FilterField>
        </div>
      ) : null}

      {activeTab === "fundamentals" ? (
        <div className="scanner-section-grid">
          <FilterField label="Listing Date" hint="Use 'From' to find newer IPOs quickly.">
            <div className="range-inputs">
              <input type="date" value={filters.listing_date_from ?? ""} onChange={updateDate("listing_date_from")} />
              <span>-</span>
              <input type="date" value={filters.listing_date_to ?? ""} onChange={updateDate("listing_date_to")} />
            </div>
          </FilterField>

          <FilterField label="Market Cap Range (Cr)">
            <RangeInputs
              minValue={filters.min_market_cap_crore}
              maxValue={filters.max_market_cap_crore}
              minStep="1"
              maxStep="1"
              onMinChange={updateNumeric("min_market_cap_crore")}
              onMaxChange={updateNumeric("max_market_cap_crore")}
            />
          </FilterField>

          <FilterField label="30 Day Avg Traded Value (Cr) >">
            <input
              type="number"
              step="0.1"
              value={toInputValue(filters.min_avg_rupee_volume_30d_crore)}
              onChange={updateNumeric("min_avg_rupee_volume_30d_crore")}
            />
          </FilterField>

          <FilterField label="20 Day Avg Turnover (Cr) >">
            <input
              type="number"
              step="0.1"
              value={toInputValue(filters.min_avg_rupee_turnover_20d_crore)}
              onChange={updateNumeric("min_avg_rupee_turnover_20d_crore")}
            />
          </FilterField>

          <FilterField label="Price Change Range (%)">
            <RangeInputs
              minValue={filters.min_change_pct}
              maxValue={filters.max_change_pct}
              onMinChange={updateNumeric("min_change_pct")}
              onMaxChange={updateNumeric("max_change_pct")}
            />
          </FilterField>
        </div>
      ) : null}

      {activeTab === "misc" ? (
        <div className="scanner-section-grid">
          <FilterField label="Trend Strength >">
            <input type="number" step="0.01" value={toInputValue(filters.min_trend_strength)} onChange={updateNumeric("min_trend_strength")} />
          </FilterField>

          <FilterField label="Max Pullback Depth %">
            <input
              type="number"
              step="0.1"
              value={toInputValue(filters.max_pullback_depth_pct)}
              onChange={updateNumeric("max_pullback_depth_pct")}
            />
          </FilterField>

          <FilterField label="Pattern Context">
            <select value={filters.pattern} onChange={updateSelect("pattern")}>
              <option value="any">Any Pattern</option>
              {patternOptions.map((option) => (
                <option key={option.id} value={option.id}>
                  {option.name}
                </option>
              ))}
            </select>
          </FilterField>

          <FilterField label="Price vs MA Mode">
            <select value={filters.price_vs_ma_mode} onChange={updateSelect("price_vs_ma_mode")}>
              <option value="any">Any</option>
              <option value="above">Above</option>
              <option value="below">Below</option>
            </select>
          </FilterField>
        </div>
      ) : null}
    </Panel>
  );
}
