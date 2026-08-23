import { useCallback, useEffect, useMemo, useState } from "react";
import { ArrowDown, ArrowUp, Plus, RefreshCw, Search, Trash2, X } from "lucide-react";
import {
  getMfAiComparison,
  getMfCategories,
  getMfPeerComparison,
  getMfPortfolio,
  getMfScreener,
  previewMfOpeningPosition,
  previewMfSip,
  saveMfPortfolio,
  type MfAiComparison,
  type MfCategoryRow,
  type MfFund,
  type MfPeerComparison,
  type MfPortfolioResponse,
  type MfPosition,
  type MfScreenerResponse,
  type MfSipFrequency,
  type MfTransaction,
} from "../lib/api";
import { FundDetailModal } from "./FundDetailModal";
import { SipCalculator } from "./SipCalculator";

import "./MutualFundsPanel.css";

/**
 * Mutual fund screener.
 *
 * Three views over one universe of ~1,000 Direct/Growth equity and hybrid
 * schemes:
 *
 * - **Screener** — the whole universe as a sortable table. Category *rank* is
 *   a first-class column, not a derived afterthought, because "3rd of 28 in
 *   small cap" is the question this page exists to answer.
 * - **Categories** — the same data pivoted category-first, for when you want
 *   to know which pocket of the market is working before picking a fund.
 * - **My portfolio** — funds actually held, valued off the same NAV series,
 *   with XIRR and a look-through to the underlying stocks.
 *
 * Everything shown is measured history. There are no projections and no
 * recommendations anywhere on this page.
 */

type PanelTab = "screener" | "categories" | "portfolio";
type ColumnSet = "returns" | "ranks" | "risk" | "rolling" | "cost";

type Column = {
  key: string;
  label: string;
  title?: string;
  align?: "left";
  format: (fund: MfFund) => React.ReactNode;
};

const num = (value: unknown, digits = 2): string =>
  typeof value === "number" && Number.isFinite(value)
    ? value.toLocaleString("en-IN", { minimumFractionDigits: digits, maximumFractionDigits: digits })
    : "—";

const pct = (value: unknown, digits = 1): string =>
  typeof value === "number" && Number.isFinite(value) ? `${value.toFixed(digits)}%` : "—";

const signedPct = (value: unknown, digits = 1): string =>
  typeof value === "number" && Number.isFinite(value)
    ? `${value >= 0 ? "+" : ""}${value.toFixed(digits)}%`
    : "—";

const crore = (value: unknown): string => {
  if (typeof value !== "number" || !Number.isFinite(value)) return "—";
  if (value >= 100000) return `${(value / 100000).toFixed(1)}L cr`;
  if (value >= 1000) return `${(value / 1000).toFixed(1)}k cr`;
  return `${value.toFixed(0)} cr`;
};

const rupees = (value: unknown): string =>
  typeof value === "number" && Number.isFinite(value)
    ? `₹${value.toLocaleString("en-IN", { maximumFractionDigits: 0 })}`
    : "—";

const tone = (value: unknown): string =>
  typeof value === "number" && Number.isFinite(value) ? (value < 0 ? "neg" : "pos") : "";

/** Rank cell: position, field size, and quartile colour in one glance. */
function rankCell(fund: MfFund, window: string): React.ReactNode {
  const rank = fund[`rank_${window}`];
  const count = fund[`rank_count_${window}`];
  if (typeof rank !== "number" || typeof count !== "number" || count < 2) return <span className="mfp-dim">—</span>;
  const quartile = Math.min(4, Math.floor(((rank - 1) / count) * 4) + 1);
  return (
    <span className={`mfp-rank q${quartile}`} title={`${rank} of ${count} funds with a ${window.toUpperCase()} record`}>
      {rank}<i>/{count}</i>
    </span>
  );
}

const NAME_COLUMN: Column = {
  key: "name",
  label: "Fund",
  align: "left",
  format: (fund) => (
    <span className="mfp-name">
      <b>{fund.name}</b>
      <small>
        {fund.amc}
        {fund.in_portfolio ? <span className="mfp-held">held</span> : null}
      </small>
    </span>
  ),
};

const CATEGORY_COLUMN: Column = {
  key: "sub_category",
  label: "Category",
  align: "left",
  format: (fund) => <span className="mfp-dim">{fund.sub_category ?? "—"}</span>,
};

const COLUMN_SETS: Record<ColumnSet, { label: string; hint: string; columns: Column[] }> = {
  returns: {
    label: "Returns",
    hint: "Point-to-point. Windows of a year or more are annualised (CAGR).",
    columns: [
      { key: "return_1m", label: "1M", format: (f) => <span className={tone(f.return_1m)}>{pct(f.return_1m)}</span> },
      { key: "return_3m", label: "3M", format: (f) => <span className={tone(f.return_3m)}>{pct(f.return_3m)}</span> },
      { key: "return_6m", label: "6M", format: (f) => <span className={tone(f.return_6m)}>{pct(f.return_6m)}</span> },
      { key: "return_1y", label: "1Y", format: (f) => <span className={tone(f.return_1y)}>{pct(f.return_1y)}</span> },
      { key: "return_3y", label: "3Y", format: (f) => <span className={tone(f.return_3y)}><b>{pct(f.return_3y)}</b></span> },
      { key: "return_5y", label: "5Y", format: (f) => <span className={tone(f.return_5y)}>{pct(f.return_5y)}</span> },
      { key: "return_10y", label: "10Y", format: (f) => <span className={tone(f.return_10y)}>{pct(f.return_10y)}</span> },
      { key: "cagr_inception", label: "Since launch", format: (f) => <span className={tone(f.cagr_inception)}>{pct(f.cagr_inception)}</span> },
    ],
  },
  ranks: {
    label: "Category rank",
    hint: "Position within the same SEBI sub-category, out of the funds that have a record over that window. Colour is the quartile.",
    columns: [
      { key: "rank_1m", label: "1M", format: (f) => rankCell(f, "1m") },
      { key: "rank_3m", label: "3M", format: (f) => rankCell(f, "3m") },
      { key: "rank_6m", label: "6M", format: (f) => rankCell(f, "6m") },
      { key: "rank_1y", label: "1Y", format: (f) => rankCell(f, "1y") },
      { key: "rank_3y", label: "3Y", format: (f) => rankCell(f, "3y") },
      { key: "rank_5y", label: "5Y", format: (f) => rankCell(f, "5y") },
      { key: "rank_10y", label: "10Y", format: (f) => rankCell(f, "10y") },
      { key: "percentile_3y", label: "3Y pctile", title: "100 = best in category", format: (f) => <span>{num(f.percentile_3y, 0)}</span> },
    ],
  },
  risk: {
    label: "Risk",
    hint: "Volatility and drawdown from daily NAV. Alpha, beta and capture are against the category's benchmark.",
    columns: [
      { key: "volatility", label: "Volatility", format: (f) => <span>{pct(f.volatility)}</span> },
      { key: "max_drawdown", label: "Worst fall", format: (f) => <span className="neg">{pct(f.max_drawdown)}</span> },
      { key: "current_drawdown", label: "Below peak", format: (f) => <span className={tone(f.current_drawdown)}>{pct(f.current_drawdown)}</span> },
      { key: "sharpe", label: "Sharpe", format: (f) => <span>{num(f.sharpe)}</span> },
      { key: "sortino", label: "Sortino", format: (f) => <span>{num(f.sortino)}</span> },
      { key: "alpha", label: "Alpha", format: (f) => <span className={tone(f.alpha)}>{pct(f.alpha)}{f.alpha_vs_price_index ? <i className="mfp-flag" title="Measured against a price index (no dividends) — flatters alpha by roughly 1.2% a year.">†</i> : null}</span> },
      { key: "beta", label: "Beta", format: (f) => <span>{num(f.beta)}</span> },
      { key: "up_capture", label: "Up capture", format: (f) => <span>{pct(f.up_capture, 0)}</span> },
      { key: "down_capture", label: "Down capture", title: "Lower is better — how much of the benchmark's falls the fund took", format: (f) => <span>{pct(f.down_capture, 0)}</span> },
    ],
  },
  rolling: {
    label: "Consistency",
    hint: "Every N-year holding period in the fund's life, annualised — not one lucky start date. 'Lost money' is the share of those periods that ended negative.",
    columns: [
      { key: "rolling3y_median", label: "3Y median", format: (f) => <span className={tone(f.rolling3y_median)}><b>{pct(f.rolling3y_median)}</b></span> },
      { key: "rolling3y_min", label: "3Y worst", format: (f) => <span className={tone(f.rolling3y_min)}>{pct(f.rolling3y_min)}</span> },
      { key: "rolling3y_max", label: "3Y best", format: (f) => <span className="pos">{pct(f.rolling3y_max)}</span> },
      { key: "rolling3y_pct_negative", label: "3Y lost money", format: (f) => <span className={(f.rolling3y_pct_negative ?? 0) > 0 ? "neg" : "mfp-dim"}>{pct(f.rolling3y_pct_negative)}</span> },
      { key: "rolling5y_median", label: "5Y median", format: (f) => <span className={tone(f.rolling5y_median)}><b>{pct(f.rolling5y_median)}</b></span> },
      { key: "rolling5y_min", label: "5Y worst", format: (f) => <span className={tone(f.rolling5y_min)}>{pct(f.rolling5y_min)}</span> },
      { key: "rolling5y_pct_negative", label: "5Y lost money", format: (f) => <span className={(f.rolling5y_pct_negative ?? 0) > 0 ? "neg" : "mfp-dim"}>{pct(f.rolling5y_pct_negative)}</span> },
    ],
  },
  cost: {
    label: "Cost & size",
    hint: "Expense ratio is charged every year, whatever the return. Turnover is how much of the portfolio was traded.",
    columns: [
      { key: "expense_ratio", label: "TER", format: (f) => <span>{pct(f.expense_ratio, 2)}</span> },
      { key: "aum_crore", label: "AUM", format: (f) => <span>{crore(f.aum_crore)}</span> },
      { key: "portfolio_turnover", label: "Turnover", format: (f) => <span>{pct(f.portfolio_turnover, 0)}</span> },
      { key: "age_years", label: "Age", format: (f) => <span>{typeof f.age_years === "number" ? `${f.age_years.toFixed(1)}y` : "—"}</span> },
      { key: "launch_date", label: "Launched", align: "left", format: (f) => <span className="mfp-dim">{f.launch_date ?? "—"}</span> },
      { key: "min_sip", label: "Min SIP", format: (f) => <span>{rupees(f.min_sip)}</span> },
      { key: "nav", label: "NAV", format: (f) => <span>{num(f.nav_latest ?? f.nav)}</span> },
    ],
  },
};

const AUM_STEPS = [
  { label: "Any size", value: null },
  { label: "≥ ₹500 cr", value: 500 },
  { label: "≥ ₹2,000 cr", value: 2000 },
  { label: "≥ ₹10,000 cr", value: 10000 },
];

const AGE_STEPS = [
  { label: "Any age", value: null },
  { label: "3y+ record", value: 3 },
  { label: "5y+ record", value: 5 },
  { label: "10y+ record", value: 10 },
];

export function MutualFundsPanel({ onOpenSymbolChart }: { onOpenSymbolChart?: (symbol: string) => void }) {
  const [tab, setTab] = useState<PanelTab>("screener");
  const [screener, setScreener] = useState<MfScreenerResponse | null>(null);
  const [categories, setCategories] = useState<MfCategoryRow[] | null>(null);
  const [portfolio, setPortfolio] = useState<MfPortfolioResponse | null>(null);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState<string | null>(null);

  const [columnSet, setColumnSet] = useState<ColumnSet>("returns");
  const [sortBy, setSortBy] = useState("return_3y");
  const [sortDir, setSortDir] = useState<"asc" | "desc">("desc");
  const [search, setSearch] = useState("");
  const [category, setCategory] = useState<string | null>("Equity");
  const [subCategories, setSubCategories] = useState<string[]>([]);
  const [amc, setAmc] = useState<string | null>(null);
  const [minAum, setMinAum] = useState<number | null>(null);
  const [minAge, setMinAge] = useState<number | null>(null);
  const [maxExpense, setMaxExpense] = useState<number | null>(null);
  const [topQuartileOnly, setTopQuartileOnly] = useState(false);
  const [heldOnly, setHeldOnly] = useState(false);
  const [openFund, setOpenFund] = useState<string | null>(null);

  const heldCodes = useMemo(
    () => new Set((portfolio?.positions ?? []).map((position) => position.scheme_code)),
    [portfolio],
  );

  const loadScreener = useCallback(() => {
    setLoading(true);
    setError(null);
    return getMfScreener({
      category,
      subCategories,
      amcs: amc ? [amc] : [],
      search: search.trim() || null,
      minAum,
      minAgeYears: minAge,
      maxExpense,
      maxQuartile: topQuartileOnly ? 1 : null,
      codes: heldOnly ? Array.from(heldCodes) : [],
      sortBy,
      sortDir,
      limit: 400,
    })
      .then((payload) => { setScreener(payload); setError(null); })
      .catch((err: unknown) => setError(err instanceof Error ? err.message : "Could not load funds."))
      .finally(() => setLoading(false));
  }, [category, subCategories, amc, search, minAum, minAge, maxExpense, topQuartileOnly, heldOnly, heldCodes, sortBy, sortDir]);

  useEffect(() => {
    // Debounced so typing in the search box does not fire a request per key.
    const timer = setTimeout(() => { void loadScreener(); }, 220);
    return () => clearTimeout(timer);
  }, [loadScreener]);

  useEffect(() => { void getMfPortfolio().then(setPortfolio).catch(() => setPortfolio(null)); }, []);

  useEffect(() => {
    if (tab === "categories" && categories === null) {
      void getMfCategories().then((payload) => setCategories(payload.categories)).catch(() => setCategories([]));
    }
  }, [tab, categories]);

  const columns = useMemo(
    () => [NAME_COLUMN, ...(columnSet === "returns" || columnSet === "ranks" ? [CATEGORY_COLUMN] : []), ...COLUMN_SETS[columnSet].columns],
    [columnSet],
  );

  const handleSort = (key: string) => {
    if (key === sortBy) {
      setSortDir((direction) => (direction === "desc" ? "asc" : "desc"));
    } else {
      setSortBy(key);
      setSortDir("desc");
    }
  };

  /** Category card -> the whole category in the screener, not just its top 5. */
  const openCategory = useCallback((subCategory: string) => {
    setSubCategories([subCategory]);
    setCategory(null);
    setSortBy("return_3y");
    setSortDir("desc");
    setTab("screener");
  }, []);

  const toggleSubCategory = (value: string) => {
    setSubCategories((current) =>
      current.includes(value) ? current.filter((item) => item !== value) : [...current, value]);
  };

  const savePositions = useCallback(async (positions: MfPosition[]) => {
    const saved = await saveMfPortfolio(positions);
    setPortfolio(saved);
    return saved;
  }, []);

  const togglePortfolio = useCallback(async (schemeCode: string) => {
    const current: MfPosition[] = (portfolio?.positions ?? []).map((position) => ({
      id: position.id,
      scheme_code: position.scheme_code,
      notes: position.notes,
      transactions: position.transactions,
    }));
    const exists = current.some((position) => position.scheme_code === schemeCode);
    const next = exists
      ? current.filter((position) => position.scheme_code !== schemeCode)
      : [...current, { scheme_code: schemeCode, transactions: [] }];
    await savePositions(next);
  }, [portfolio, savePositions]);

  const subCategoryOptions = useMemo(() => {
    const facets = screener?.facets?.sub_categories ?? {};
    return Object.entries(facets)
      .filter(([, meta]) => !category || meta.category === category)
      .map(([name, meta]) => ({ name, count: meta.count }));
  }, [screener, category]);

  return (
    <div className="mfp">
      <header className="mfp-head">
        <div className="mfp-head-title">
          <h2>Mutual funds</h2>
          <p>
            {screener
              ? `${screener.total.toLocaleString("en-IN")} Direct-plan Growth schemes · NAV as of ${screener.as_of ?? "—"}`
              : "Loading universe…"}
          </p>
        </div>
        <nav className="mfp-tabs">
          {([
            ["screener", "Screener"],
            ["categories", "Categories"],
            ["portfolio", `My portfolio${portfolio?.positions?.length ? ` (${portfolio.positions.length})` : ""}`],
          ] as const).map(([key, label]) => (
            <button
              key={key}
              type="button"
              className={tab === key ? "mfp-tab active" : "mfp-tab"}
              onClick={() => setTab(key)}
            >
              {label}
            </button>
          ))}
        </nav>
      </header>

      {error ? (
        <div className="mfp-error">
          <span>{error}</span>
          <button type="button" onClick={() => { void loadScreener(); }} disabled={loading}>
            <RefreshCw size={12} /> {loading ? "Retrying…" : "Retry"}
          </button>
        </div>
      ) : null}

      {/* ==================================================== screener tab */}
      {tab === "screener" ? (
        <>
          <div className="mfp-filters">
            <div className="mfp-search">
              <Search size={13} />
              <input
                placeholder="Search fund or AMC…"
                value={search}
                onChange={(event) => setSearch(event.target.value)}
              />
              {search ? (
                <button type="button" onClick={() => setSearch("")} aria-label="Clear"><X size={12} /></button>
              ) : null}
            </div>

            <div className="mfp-filter-row">
              <div className="mfp-seg">
                {["Equity", "Hybrid"].map((value) => (
                  <button
                    key={value}
                    type="button"
                    className={category === value ? "active" : ""}
                    onClick={() => { setCategory(category === value ? null : value); setSubCategories([]); }}
                  >
                    {value}
                    <i>{screener?.facets?.categories?.[value] ?? 0}</i>
                  </button>
                ))}
                <button
                  type="button"
                  className={category === null ? "active" : ""}
                  onClick={() => { setCategory(null); setSubCategories([]); }}
                >
                  All
                </button>
              </div>

              <select
                className="mfp-select"
                value={amc ?? ""}
                onChange={(event) => setAmc(event.target.value || null)}
              >
                <option value="">All fund houses</option>
                {Object.entries(screener?.facets?.amcs ?? {}).map(([name, count]) => (
                  <option key={name} value={name}>{name} ({count})</option>
                ))}
              </select>

              <select
                className="mfp-select"
                value={minAum ?? ""}
                onChange={(event) => setMinAum(event.target.value ? Number(event.target.value) : null)}
              >
                {AUM_STEPS.map((step) => (
                  <option key={step.label} value={step.value ?? ""}>{step.label}</option>
                ))}
              </select>

              <select
                className="mfp-select"
                value={minAge ?? ""}
                onChange={(event) => setMinAge(event.target.value ? Number(event.target.value) : null)}
              >
                {AGE_STEPS.map((step) => (
                  <option key={step.label} value={step.value ?? ""}>{step.label}</option>
                ))}
              </select>

              <label className="mfp-inline">
                TER ≤
                <input
                  type="number"
                  step="0.05"
                  min="0"
                  placeholder="any"
                  value={maxExpense ?? ""}
                  onChange={(event) => setMaxExpense(event.target.value ? Number(event.target.value) : null)}
                />
                %
              </label>

              <button
                type="button"
                className={topQuartileOnly ? "mfp-toggle active" : "mfp-toggle"}
                onClick={() => setTopQuartileOnly((value) => !value)}
                title="Only funds in the top quartile of their category on 3-year CAGR"
              >
                Top quartile
              </button>

              <button
                type="button"
                className={heldOnly ? "mfp-toggle active" : "mfp-toggle"}
                onClick={() => setHeldOnly((value) => !value)}
                disabled={!heldCodes.size}
                title={heldCodes.size ? "Only funds in my portfolio" : "Add funds to your portfolio first"}
              >
                My funds
              </button>

              <button type="button" className="mfp-toggle" onClick={() => void loadScreener()} title="Reload">
                <RefreshCw size={12} />
              </button>
            </div>

            {subCategoryOptions.length ? (
              <div className="mfp-subcats">
                {subCategoryOptions.map((option) => (
                  <button
                    key={option.name}
                    type="button"
                    className={subCategories.includes(option.name) ? "mfp-chip active" : "mfp-chip"}
                    onClick={() => toggleSubCategory(option.name)}
                  >
                    {option.name}<i>{option.count}</i>
                  </button>
                ))}
                {subCategories.length ? (
                  <button type="button" className="mfp-chip clear" onClick={() => setSubCategories([])}>
                    Clear
                  </button>
                ) : null}
              </div>
            ) : null}
          </div>

          <div className="mfp-colsets">
            {(Object.keys(COLUMN_SETS) as ColumnSet[]).map((key) => (
              <button
                key={key}
                type="button"
                className={columnSet === key ? "mfp-pill active" : "mfp-pill"}
                onClick={() => setColumnSet(key)}
              >
                {COLUMN_SETS[key].label}
              </button>
            ))}
            <span className="mfp-colset-hint">{COLUMN_SETS[columnSet].hint}</span>
          </div>

          <div className="mfp-table-wrap">
            <table className="mfp-table">
              <thead>
                <tr>
                  <th className="mfp-th-idx">#</th>
                  {columns.map((column) => (
                    <th
                      key={column.key}
                      className={column.align === "left" ? "left sortable" : "sortable"}
                      title={column.title}
                      onClick={() => handleSort(column.key)}
                    >
                      {column.label}
                      {sortBy === column.key ? (
                        sortDir === "desc" ? <ArrowDown size={10} /> : <ArrowUp size={10} />
                      ) : null}
                    </th>
                  ))}
                  <th />
                </tr>
              </thead>
              <tbody>
                {(screener?.funds ?? []).map((fund, index) => (
                  <tr
                    key={fund.scheme_code}
                    className={heldCodes.has(fund.scheme_code) ? "mfp-row-held" : undefined}
                    onClick={() => setOpenFund(fund.scheme_code)}
                  >
                    <td className="mfp-dim mfp-th-idx">{(screener?.offset ?? 0) + index + 1}</td>
                    {columns.map((column) => (
                      <td key={column.key} className={column.align === "left" ? "left" : undefined}>
                        {column.format(fund)}
                      </td>
                    ))}
                    <td>
                      <button
                        type="button"
                        className={heldCodes.has(fund.scheme_code) ? "mfp-add active" : "mfp-add"}
                        title={heldCodes.has(fund.scheme_code) ? "Remove from my portfolio" : "Add to my portfolio"}
                        onClick={(event) => { event.stopPropagation(); void togglePortfolio(fund.scheme_code); }}
                      >
                        {heldCodes.has(fund.scheme_code) ? <Trash2 size={11} /> : <Plus size={11} />}
                      </button>
                    </td>
                  </tr>
                ))}
              </tbody>
            </table>

            {loading && !screener ? <div className="mfp-placeholder">Loading funds…</div> : null}
            {!loading && screener && !screener.funds.length ? (
              <div className="mfp-placeholder">No fund matches these filters.</div>
            ) : null}
            {screener && screener.returned < screener.total ? (
              <p className="mfp-note">
                Showing the top {screener.returned} of {screener.total} matches by{" "}
                {screener.sort_by.replace(/_/g, " ")}. Narrow the filters to see the rest.
              </p>
            ) : null}
          </div>
        </>
      ) : null}

      {/* ================================================== categories tab */}
      {tab === "categories" ? (
        <div className="mfp-cats">
          {categories === null ? <div className="mfp-placeholder">Loading categories…</div> : null}
          {categories?.map((row) => (
            <section className="mfp-cat" key={row.sub_category}>
              <header>
                <div>
                  <button type="button" className="mfp-cat-open" onClick={() => openCategory(row.sub_category)}>
                    <h3>{row.sub_category}</h3>
                  </button>
                  <small>
                    {row.category} · {row.count} funds · vs {row.benchmark_label}
                    {/* Hybrid benchmark labels already carry "(reference)" for
                        the chart legend, so only add it when they do not. */}
                    {row.benchmark_is_reference_only && !/reference/i.test(row.benchmark_label ?? "")
                      ? " (reference)"
                      : ""}
                  </small>
                </div>
                <div className="mfp-cat-nums">
                  <span><i>1Y avg</i><b className={tone(row.avg_return_1y)}>{pct(row.avg_return_1y)}</b></span>
                  <span><i>3Y avg</i><b className={tone(row.avg_return_3y)}>{pct(row.avg_return_3y)}</b></span>
                  <span><i>5Y avg</i><b className={tone(row.avg_return_5y)}>{pct(row.avg_return_5y)}</b></span>
                  <span><i>Avg TER</i><b>{pct(row.avg_expense_ratio, 2)}</b></span>
                  <span><i>Avg worst fall</i><b className="neg">{pct(row.avg_max_drawdown)}</b></span>
                </div>
              </header>
              <ol className="mfp-leaders">
                {/* The card shows the top 5; the whole field is one click away. */}
                {row.leaders.map((leader, index) => (
                  <li key={leader.scheme_code}>
                    <span className="mfp-leader-rank">{index + 1}</span>
                    <button type="button" className="mfp-leader-name" onClick={() => setOpenFund(leader.scheme_code)}>
                      {leader.name}
                    </button>
                    <span className="mfp-dim">{leader.amc}</span>
                    <span className={tone(leader.return_3y)}>{pct(leader.return_3y)}</span>
                    <span className="mfp-dim">TER {pct(leader.expense_ratio, 2)}</span>
                  </li>
                ))}
              </ol>
              <button type="button" className="mfp-cat-all" onClick={() => openCategory(row.sub_category)}>
                View all {row.count} funds in {row.sub_category} →
              </button>
            </section>
          ))}
        </div>
      ) : null}

      {/* =================================================== portfolio tab */}
      {tab === "portfolio" ? (
        <PortfolioView
          portfolio={portfolio}
          universe={screener?.funds ?? []}
          onSave={savePositions}
          onOpenFund={setOpenFund}
          onOpenSymbolChart={onOpenSymbolChart}
        />
      ) : null}

      {openFund ? (
        <FundDetailModal
          schemeCode={openFund}
          onClose={() => setOpenFund(null)}
          onOpenSymbolChart={onOpenSymbolChart}
          onTogglePortfolio={(code) => void togglePortfolio(code)}
          inPortfolio={heldCodes.has(openFund)}
        />
      ) : null}
    </div>
  );
}

/* ========================================================================= */
/* Portfolio                                                                 */
/* ========================================================================= */

function PortfolioView({
  portfolio,
  universe,
  onSave,
  onOpenFund,
  onOpenSymbolChart,
}: {
  portfolio: MfPortfolioResponse | null;
  universe: MfFund[];
  onSave: (positions: MfPosition[]) => Promise<MfPortfolioResponse>;
  onOpenFund: (schemeCode: string) => void;
  onOpenSymbolChart?: (symbol: string) => void;
}) {
  const [editing, setEditing] = useState<string | null>(null);
  const [busy, setBusy] = useState(false);
  // Two-step delete. A position can hold 180 weekly instalments, and losing
  // that to a stray click on an 11px icon is not a recoverable mistake.
  const [confirmDelete, setConfirmDelete] = useState<string | null>(null);
  const [comparison, setComparison] = useState<MfPeerComparison | null>(null);
  const [aiComparison, setAiComparison] = useState<MfAiComparison | null>(null);
  const [aiBusy, setAiBusy] = useState(false);

  const positions = portfolio?.positions ?? [];
  const totals = portfolio?.totals;
  const allocation = portfolio?.allocation;

  const asPositions = useCallback((): MfPosition[] =>
    positions.map((position) => ({
      id: position.id,
      scheme_code: position.scheme_code,
      notes: position.notes,
      transactions: position.transactions,
    })), [positions]);

  const commit = useCallback(async (next: MfPosition[]) => {
    setBusy(true);
    try { await onSave(next); } finally { setBusy(false); }
  }, [onSave]);

  const replaceTransactions = useCallback(async (schemeCode: string, transactions: MfTransaction[]) => {
    const next = asPositions().map((position) =>
      position.scheme_code === schemeCode ? { ...position, transactions } : position);
    await commit(next);
  }, [asPositions, commit]);

  const removePosition = useCallback(async (schemeCode: string) => {
    await commit(asPositions().filter((position) => position.scheme_code !== schemeCode));
    setConfirmDelete(null);
    if (editing === schemeCode) setEditing(null);
  }, [asPositions, commit, editing]);

  const positionKey = positions.map((p) => p.scheme_code).join(",");
  useEffect(() => {
    if (!positions.length) { setComparison(null); return; }
    let cancelled = false;
    getMfPeerComparison()
      .then((payload) => { if (!cancelled) setComparison(payload); })
      .catch(() => { if (!cancelled) setComparison(null); });
    return () => { cancelled = true; };
  }, [positionKey, positions.length]);

  const loadAiComparison = useCallback(() => {
    setAiBusy(true);
    getMfAiComparison()
      .then(setAiComparison)
      .catch((error: unknown) => setAiComparison({
        available: false,
        reason: error instanceof Error ? error.message : "Could not generate the comparison.",
      }))
      .finally(() => setAiBusy(false));
  }, []);

  if (!positions.length) {
    return (
      <>
        <div className="mfp-placeholder mfp-placeholder-tall">
          <p><b>No funds in your portfolio yet.</b></p>
          <p>
            Add one from the Screener with the <Plus size={11} /> button, then record what you hold —
            units you already own as of a date, a lumpsum, or a recurring SIP (weekly, fortnightly,
            monthly or quarterly). Units, current value, P&amp;L and XIRR are all computed from the
            same AMFI NAV series the charts use.
          </p>
        </div>
        <SipCalculator />
      </>
    );
  }

  return (
    <div className="mfp-portfolio">
      <div className="mfp-pf-totals">
        <div><span>Invested</span><strong>{rupees(totals?.invested)}</strong></div>
        <div><span>Current value</span><strong>{rupees(totals?.current_value)}</strong></div>
        <div>
          <span>P&amp;L</span>
          <strong className={tone(totals?.gain)}>
            {rupees(totals?.gain)} <em>{signedPct(totals?.gain_pct)}</em>
          </strong>
          <small>current value less what you put in</small>
        </div>
        <div>
          <span>XIRR</span>
          <strong className={tone(totals?.xirr)}>{pct(totals?.xirr, 2)}</strong>
          <small>money-weighted, annualised</small>
        </div>
        <div><span>Funds</span><strong>{totals?.position_count ?? 0}</strong><small>valued {portfolio?.as_of ?? "—"}</small></div>
      </div>

      <div className="mfp-table-wrap">
        <table className="mfp-table">
          <thead>
            <tr>
              <th className="left">Fund</th>
              <th>Units</th><th>Avg cost</th><th>NAV</th>
              <th>Invested</th><th>Current</th><th>P&amp;L</th><th>XIRR</th><th>Weight</th><th>Txns</th><th />
            </tr>
          </thead>
          <tbody>
            {positions.map((position) => (
              <tr key={position.scheme_code}>
                <td className="left">
                  <button type="button" className="mfp-leader-name" onClick={() => onOpenFund(position.scheme_code)}>
                    {position.fund?.name ?? position.scheme_code}
                  </button>
                  <small className="mfp-dim"> {position.fund?.sub_category ?? ""}</small>
                  {position.unpriced_transactions ? (
                    <small className="neg"> · {position.unpriced_transactions} txn(s) outside NAV history</small>
                  ) : null}
                </td>
                <td>{num(position.units, 3)}</td>
                <td>{num(position.avg_cost_nav)}</td>
                <td>{num(position.latest_nav)}</td>
                <td>{rupees(position.invested)}</td>
                <td>{rupees(position.current_value)}</td>
                <td className={tone(position.gain)}>{rupees(position.gain)} <em>{signedPct(position.gain_pct)}</em></td>
                <td className={tone(position.xirr)}>{pct(position.xirr, 2)}</td>
                <td className="mfp-dim">{pct(position.weight_pct)}</td>
                <td className="mfp-dim">{position.transaction_count}</td>
                <td className="mfp-pf-actions">
                  <button
                    type="button"
                    className="mfp-add"
                    onClick={() => setEditing(editing === position.scheme_code ? null : position.scheme_code)}
                  >
                    {editing === position.scheme_code ? "Done" : "Edit"}
                  </button>
                  {confirmDelete === position.scheme_code ? (
                    <>
                      <button
                        type="button"
                        className="mfp-add mfp-add-danger"
                        disabled={busy}
                        onClick={() => void removePosition(position.scheme_code)}
                      >
                        Delete {position.transaction_count > 0 ? `${position.transaction_count} txns` : ""}?
                      </button>
                      <button type="button" className="mfp-add" onClick={() => setConfirmDelete(null)}>
                        Keep
                      </button>
                    </>
                  ) : (
                    <button
                      type="button"
                      className="mfp-add"
                      title="Remove this fund from your portfolio"
                      onClick={() => setConfirmDelete(position.scheme_code)}
                    >
                      <Trash2 size={11} /> Remove
                    </button>
                  )}
                </td>
              </tr>
            ))}
          </tbody>
        </table>
      </div>

      {editing ? (
        <TransactionEditor
          schemeCode={editing}
          name={positions.find((position) => position.scheme_code === editing)?.fund?.name ?? editing}
          transactions={positions.find((position) => position.scheme_code === editing)?.transactions ?? []}
          busy={busy}
          onChange={(transactions) => void replaceTransactions(editing, transactions)}
          onClose={() => setEditing(null)}
        />
      ) : null}

      <div className="mfp-pf-grid">
        <section className="mfp-pf-card">
          <h3>By category</h3>
          {Object.entries(allocation?.by_sub_category ?? {}).map(([name, value]) => (
            <div className="mfp-bar-row" key={name}>
              <span>{name}</span>
              <i className="mfp-bar"><i style={{ width: `${Math.min(100, (value / (totals?.current_value || 1)) * 100)}%` }} /></i>
              <em>{pct((value / (totals?.current_value || 1)) * 100)}</em>
            </div>
          ))}
        </section>

        <section className="mfp-pf-card">
          <h3>By fund house</h3>
          {Object.entries(allocation?.by_amc ?? {}).map(([name, value]) => (
            <div className="mfp-bar-row" key={name}>
              <span>{name}</span>
              <i className="mfp-bar"><i style={{ width: `${Math.min(100, (value / (totals?.current_value || 1)) * 100)}%` }} /></i>
              <em>{pct((value / (totals?.current_value || 1)) * 100)}</em>
            </div>
          ))}
        </section>
      </div>

      <section className="mfp-pf-card">
        <div className="mfp-pf-card-head">
          <h3>How your funds compare in their category</h3>
          {!aiComparison ? (
            <button type="button" className="mfp-toggle active" disabled={aiBusy} onClick={loadAiComparison}>
              {aiBusy ? "Writing…" : "Get the read"}
            </button>
          ) : null}
        </div>

        {comparison?.holdings?.length ? (
          <div className="mfp-table-wrap">
            <table className="mfp-table">
              <thead>
                <tr>
                  <th className="left">Your fund</th><th>Category</th><th>3Y</th>
                  <th>Category avg</th><th>Rank</th><th>Standing</th><th>Trend</th>
                  <th title="Funds in the same category with a higher 3-year return">Beaten by</th>
                </tr>
              </thead>
              <tbody>
                {comparison.holdings.map((row) => (
                  <tr key={row.scheme_code} onClick={() => onOpenFund(row.scheme_code)}>
                    <td className="left"><b>{row.name ?? row.scheme_code}</b></td>
                    <td className="mfp-dim">{row.sub_category}</td>
                    <td className={tone(row.return_3y)}><b>{pct(row.return_3y)}</b></td>
                    <td className="mfp-dim">{pct(row.category_avg_3y)}</td>
                    <td>
                      {row.rank_3y && row.rank_count_3y ? `${row.rank_3y} / ${row.rank_count_3y}` : "—"}
                    </td>
                    <td className={
                      (row.measured_standing ?? 50) >= 60 ? "pos"
                        : (row.measured_standing ?? 50) <= 40 ? "neg" : ""
                    }>
                      {row.measured_standing != null ? row.measured_standing.toFixed(0) : "—"}
                    </td>
                    <td className={
                      row.trajectory === "slipping" ? "neg" : row.trajectory === "improving" ? "pos" : "mfp-dim"
                    }>
                      {row.trajectory ?? "—"}
                    </td>
                    <td className={(row.better_on_3y_count ?? 0) > 0 ? "neg" : "pos"}>
                      {row.better_on_3y_count ?? 0} of {row.peer_count ?? 0}
                    </td>
                  </tr>
                ))}
              </tbody>
            </table>
          </div>
        ) : (
          <p className="mfp-note">Measuring your funds against their categories…</p>
        )}

        {comparison?.holdings?.some((h) => h.peers_ahead.length) ? (
          <div className="mfp-ahead">
            {comparison.holdings.filter((h) => h.peers_ahead.length).map((holding) => (
              <div className="mfp-ahead-group" key={holding.scheme_code}>
                <h4>
                  Better than your <b>{holding.name}</b> on return, cost <i>and</i> worst fall
                </h4>
                <div className="mfp-table-wrap">
                  <table className="mfp-table">
                    <thead>
                      <tr>
                        <th className="left">Fund</th><th>3Y</th><th>Ahead by</th>
                        <th>TER</th><th>Worst fall</th><th>Sharpe</th>
                      </tr>
                    </thead>
                    <tbody>
                      {holding.peers_ahead.map((peer) => (
                        <tr key={peer.scheme_code} onClick={() => onOpenFund(peer.scheme_code)}>
                          <td className="left">{peer.name}<br /><small className="mfp-dim">{peer.amc}</small></td>
                          <td className={tone(peer.return_3y)}><b>{pct(peer.return_3y)}</b></td>
                          <td className="pos">+{num(peer.return_gap)}%</td>
                          <td>{pct(peer.expense_ratio, 2)}</td>
                          <td className="neg">{pct(peer.max_drawdown)}</td>
                          <td>{num(peer.sharpe)}</td>
                        </tr>
                      ))}
                    </tbody>
                  </table>
                </div>
              </div>
            ))}
          </div>
        ) : null}

        {aiComparison ? (
          aiComparison.available && aiComparison.note ? (
            <div className="mfp-ai">
              {aiComparison.note.headline ? (
                <p className="mfp-ai-headline">{aiComparison.note.headline}</p>
              ) : null}
              {(aiComparison.note.overview ?? []).map((paragraph) => (
                <p className="mfp-ai-prose" key={paragraph}>{paragraph}</p>
              ))}
              {(aiComparison.note.per_fund ?? []).map((entry) => (
                <div className="mfp-ai-fund" key={entry.fund}>
                  <h4>
                    {entry.fund}
                    {entry.standing ? <span className={
                      /behind/i.test(entry.standing) ? "mfp-tag-neg"
                        : /ahead/i.test(entry.standing) ? "mfp-tag-pos" : "mfp-tag"
                    }>{entry.standing}</span> : null}
                  </h4>
                  {entry.opinion ? <p className="mfp-ai-prose">{entry.opinion}</p> : null}
                  {entry.better_performers?.length ? (
                    <ul className="mfp-ai-list">
                      {entry.better_performers.map((item) => <li key={item}>{item}</li>)}
                    </ul>
                  ) : null}
                </div>
              ))}
              {aiComparison.note.caveat ? <p className="mfp-note">{aiComparison.note.caveat}</p> : null}
              <p className="mfp-note">
                Written from the measured figures above. It names what did better and gives a candid
                read on each record — it will not tell you whether to switch, or how much to invest.
              </p>
            </div>
          ) : (
            <p className="mfp-note">{aiComparison.reason ?? "Comparison unavailable."}</p>
          )
        ) : (
          <p className="mfp-note">
            Names the funds in each category that beat yours, and gives a straight read on whether a
            weak record looks persistent or like a rough patch.
          </p>
        )}
      </section>

      <SipCalculator />

      <section className="mfp-pf-card">
        <div className="mfp-pf-card-head">
          <h3>Look-through: what you actually own</h3>
          <span className="mfp-dim">
            {allocation?.look_through_count ?? 0} distinct stocks across your funds
          </span>
        </div>
        <p className="mfp-note">
          Your funds' holdings collapsed into single positions. Five funds each holding a stock at 8%
          is one concentrated bet, not five diversified ones — this is the only place that shows it.
        </p>
        <div className="mfp-table-wrap">
          <table className="mfp-table">
            <thead>
              <tr>
                <th className="mfp-th-idx">#</th><th className="left">Stock</th><th>Weight</th>
                <th>Value</th><th>Sector</th><th>Cap</th><th>In funds</th><th />
              </tr>
            </thead>
            <tbody>
              {(allocation?.look_through_top ?? []).map((row, index) => (
                <tr key={row.name}>
                  <td className="mfp-dim mfp-th-idx">{index + 1}</td>
                  <td className="left">{row.name}</td>
                  <td><b>{pct(row.weight_pct, 2)}</b></td>
                  <td>{rupees(row.value)}</td>
                  <td className="mfp-dim">{row.sector ?? "—"}</td>
                  <td className="mfp-dim">{row.cap_class ?? "—"}</td>
                  <td
                    className="mfp-dim"
                    title={row.funds.map((fund) => `${fund.name}: ${pct(fund.weight_pct, 2)}`).join("\n")}
                  >
                    {row.fund_count}
                  </td>
                  <td>
                    {row.symbol && onOpenSymbolChart ? (
                      <button type="button" className="mfp-add" onClick={() => onOpenSymbolChart(row.symbol as string)}>
                        {row.symbol}
                      </button>
                    ) : null}
                  </td>
                </tr>
              ))}
            </tbody>
          </table>
        </div>
      </section>
    </div>
  );
}

/* ------------------------------------------------------------------------- */

function TransactionEditor({
  schemeCode,
  name,
  transactions,
  busy,
  onChange,
  onClose,
}: {
  schemeCode: string;
  name: string;
  transactions: MfTransaction[];
  busy: boolean;
  onChange: (transactions: MfTransaction[]) => void;
  onClose: () => void;
}) {
  const today = new Date().toISOString().slice(0, 10);
  const [mode, setMode] = useState<"holding" | "lumpsum" | "sip">("holding");
  const [date, setDate] = useState(today);
  const [endDate, setEndDate] = useState(today);
  const [amount, setAmount] = useState("");
  const [units, setUnits] = useState("");
  const [frequency, setFrequency] = useState<MfSipFrequency>("weekly");
  const [note, setNote] = useState<string | null>(null);
  const [working, setWorking] = useState(false);

  const run = async (task: () => Promise<void>) => {
    setWorking(true);
    setNote(null);
    try { await task(); } catch (error) {
      setNote(error instanceof Error ? error.message : "Could not add that.");
    } finally { setWorking(false); }
  };

  /** Units already held as of a date — priced at that date's NAV server-side. */
  const addHolding = () => run(async () => {
    const held = Number(units);
    if (!Number.isFinite(held) || held <= 0) { setNote("Enter the number of units you hold."); return; }
    const preview = await previewMfOpeningPosition({ units: held, as_of: date });
    if (!preview.count) { setNote("That date is outside this fund's NAV history."); return; }
    onChange([...transactions, ...preview.transactions]);
    setUnits("");
    setNote(`Opening holding of ${held.toLocaleString("en-IN")} units recorded at the ${date} NAV.`);
  });

  const addLumpsum = () => run(async () => {
    const value = Number(amount);
    if (!Number.isFinite(value) || value <= 0) { setNote("Enter an amount."); return; }
    onChange([...transactions, { date, type: "buy", amount: value }]);
    setAmount("");
    setNote(`Added ₹${value.toLocaleString("en-IN")} on ${date}.`);
  });

  const addSip = () => run(async () => {
    const value = Number(amount);
    if (!Number.isFinite(value) || value <= 0) { setNote("Enter the instalment amount."); return; }
    const preview = await previewMfSip({
      start_date: date, end_date: endDate, amount: value, frequency,
    });
    if (!preview.count) { setNote("That date range produces no instalments."); return; }
    onChange([...transactions, ...preview.transactions]);
    setAmount("");
    setNote(
      `Added ${preview.count} ${frequency} instalments, ${preview.first_date} to ${preview.last_date}` +
      ` — ₹${preview.total_amount.toLocaleString("en-IN")} in total.`,
    );
  });

  return (
    <section className="mfp-editor">
      <header>
        <h3>{name}</h3>
        <button type="button" className="mfp-add" onClick={onClose}><X size={12} /></button>
      </header>

      <div className="mfp-seg">
        <button type="button" className={mode === "holding" ? "active" : ""} onClick={() => setMode("holding")}>
          Units I already hold
        </button>
        <button type="button" className={mode === "lumpsum" ? "active" : ""} onClick={() => setMode("lumpsum")}>
          One-off purchase
        </button>
        <button type="button" className={mode === "sip" ? "active" : ""} onClick={() => setMode("sip")}>
          Recurring SIP
        </button>
      </div>

      {mode === "holding" ? (
        <>
          <div className="mfp-editor-form">
            <label className="mfp-inline">
              Units held
              <input
                type="number" step="0.001" min="0" placeholder="e.g. 1234.567"
                value={units} onChange={(event) => setUnits(event.target.value)}
              />
            </label>
            <label className="mfp-inline">
              As of
              <input type="date" value={date} max={today} onChange={(event) => setDate(event.target.value)} />
            </label>
            <button type="button" className="mfp-toggle active" disabled={busy || working} onClick={addHolding}>
              Add holding
            </button>
          </div>
          <p className="mfp-note">
            Use this when you have been investing for a while and do not want to key in every past
            instalment. The units are valued at that date's NAV, so P&amp;L and XIRR measure
            performance <b>since that date</b> — not since your original purchases. Add your ongoing
            SIP on top and it carries on from there.
          </p>
        </>
      ) : null}

      {mode === "lumpsum" ? (
        <div className="mfp-editor-form">
          <label className="mfp-inline">
            Date
            <input type="date" value={date} max={today} onChange={(event) => setDate(event.target.value)} />
          </label>
          <label className="mfp-inline">
            ₹
            <input
              type="number" min="1" placeholder="amount"
              value={amount} onChange={(event) => setAmount(event.target.value)}
            />
          </label>
          <button type="button" className="mfp-toggle active" disabled={busy || working} onClick={addLumpsum}>
            Add
          </button>
        </div>
      ) : null}

      {mode === "sip" ? (
        <>
          <div className="mfp-editor-form">
            <div className="mfp-seg">
              {(["weekly", "fortnightly", "monthly", "quarterly"] as MfSipFrequency[]).map((option) => (
                <button
                  key={option}
                  type="button"
                  className={frequency === option ? "active" : ""}
                  onClick={() => setFrequency(option)}
                >
                  {option[0].toUpperCase() + option.slice(1)}
                </button>
              ))}
            </div>
            <label className="mfp-inline">
              From
              <input type="date" value={date} max={today} onChange={(event) => setDate(event.target.value)} />
            </label>
            <label className="mfp-inline">
              To
              <input type="date" value={endDate} max={today} onChange={(event) => setEndDate(event.target.value)} />
            </label>
            <label className="mfp-inline">
              ₹
              <input
                type="number" min="1" placeholder="per instalment"
                value={amount} onChange={(event) => setAmount(event.target.value)}
              />
            </label>
            <button type="button" className="mfp-toggle active" disabled={busy || working} onClick={addSip}>
              Add instalments
            </button>
          </div>
          <p className="mfp-note">
            A weekly SIP lands on the same weekday as the start date. Each instalment is priced at
            that day's NAV, which is what makes the XIRR real rather than an average-cost
            approximation.
          </p>
        </>
      ) : null}

      {note ? <p className="mfp-editor-note">{note}</p> : null}

      {transactions.length ? (
        <div className="mfp-table-wrap mfp-editor-list">
          <table className="mfp-table">
            <thead>
              <tr><th className="left">Date</th><th>Type</th><th>Amount</th><th>Units</th><th /></tr>
            </thead>
            <tbody>
              {transactions.map((transaction, index) => (
                <tr key={transaction.id ?? `${transaction.date}-${index}`}>
                  <td className="left">{transaction.date}</td>
                  <td>{transaction.units && !transaction.amount ? "holding" : transaction.type}</td>
                  <td>{transaction.amount ? rupees(transaction.amount) : "at NAV"}</td>
                  <td>{transaction.units ? num(transaction.units, 3) : "—"}</td>
                  <td>
                    <button
                      type="button"
                      className="mfp-add"
                      title="Remove"
                      onClick={() => onChange(transactions.filter((_, position) => position !== index))}
                    >
                      <Trash2 size={11} />
                    </button>
                  </td>
                </tr>
              ))}
            </tbody>
          </table>
        </div>
      ) : null}
      {transactions.length > 1 ? (
        <p className="mfp-note">
          {transactions.length} transactions recorded. Clear them all and re-add if you want to start over.
        </p>
      ) : null}
    </section>
  );
}
