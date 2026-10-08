import { useEffect, useMemo, useState, type ReactNode } from "react";

import {
  getPeerMetrics,
  type CompanyFundamentals,
  type IndustryGroupStockItem,
  type MarketKey,
  type PeerMetricsItem,
  type QuarterlyResultItem,
  type StockOverview,
} from "../lib/api";

import "./ResearchFundamentals.css";

/**
 * The research layout's right pane: fundamentals in Screener.in-style tabs.
 *
 * Production often has only the quarterly results (gotcha 137 — Screener
 * refuses the Space and BSE filings are the fallback), so About, Quarters and
 * Profit & Loss are always present and the other statement tabs appear only
 * when the payload carries them. Profit & Loss falls back to fiscal years
 * summed from four filed quarters, and says so.
 */

type TabKey = "about" | "quarters" | "pnl" | "peers" | "balance" | "cashflow" | "ratios" | "shareholding" | "updates" | "details";

type ResearchFundamentalsProps = {
  symbol: string | null;
  fundamentals: CompanyFundamentals | null;
  loading: boolean;
  error: string | null;
  summary: StockOverview | null;
  market: MarketKey;
  /** The stock's industry group, from the groups payload. */
  peers: { groupName: string; members: IndustryGroupStockItem[] } | "unavailable" | null;
  onSelectSymbol?: (symbol: string) => void;
  /** The full card view (management, triggers, insider trades…) mounts here. */
  onDetailsMount?: (node: HTMLDivElement | null) => void;
};

const TAB_STORAGE_KEY = "mr-malik-research-fund-tab:v1";
const MONTHS: Record<string, number> = {
  jan: 1, feb: 2, mar: 3, apr: 4, may: 5, jun: 6, jul: 7, aug: 8, sep: 9, oct: 10, nov: 11, dec: 12,
};

function parsePeriod(period: string): { year: number; month: number } | null {
  const match = /([A-Za-z]{3})[a-z]*[\s'-]*(\d{2,4})/.exec(period ?? "");
  if (!match) return null;
  const month = MONTHS[match[1].toLowerCase()];
  if (!month) return null;
  let year = Number(match[2]);
  if (year < 100) year += 2000;
  return { year, month };
}

function periodKey(period: string) {
  const p = parsePeriod(period);
  return p ? p.year * 12 + p.month : Number.NaN;
}

function num(value: number | null | undefined): value is number {
  return typeof value === "number" && Number.isFinite(value);
}

function fmtNumber(value: number | null | undefined, digits = 0) {
  if (!num(value)) return "—";
  return value.toLocaleString("en-IN", { minimumFractionDigits: digits, maximumFractionDigits: digits });
}

function fmtCrore(value: number | null | undefined) {
  if (!num(value)) return "—";
  return fmtNumber(value, Math.abs(value) < 100 ? 2 : 0);
}

function fmtPct(value: number | null | undefined, digits = 1) {
  if (!num(value)) return "—";
  return `${value.toFixed(digits)}%`;
}

function fmtSignedPct(value: number | null | undefined) {
  if (!num(value)) return "";
  return `${value > 0 ? "+" : ""}${value.toFixed(0)}%`;
}

function growth(current: number | null | undefined, base: number | null | undefined) {
  if (!num(current) || !num(base) || base === 0) return null;
  return ((current - base) / Math.abs(base)) * 100;
}

function readTab(): TabKey {
  try {
    const raw = window.localStorage.getItem(TAB_STORAGE_KEY);
    if (raw) return raw as TabKey;
  } catch {
    // storage blocked
  }
  return "about";
}

type QuarterRow = QuarterlyResultItem & { key: number };

/** Oldest first, deduplicated, with YoY derived where the payload omits it. */
function prepareQuarters(items: QuarterlyResultItem[]): QuarterRow[] {
  const byKey = new Map<number, QuarterRow>();
  for (const item of items ?? []) {
    const key = periodKey(item.period);
    if (Number.isFinite(key) && !byKey.has(key)) byKey.set(key, { ...item, key });
  }
  const rows = [...byKey.values()].sort((a, b) => a.key - b.key);
  return rows.map((row) => {
    const yearAgo = byKey.get(row.key - 12);
    const prev = rows[rows.indexOf(row) - 1];
    return {
      ...row,
      sales_yoy_pct: row.sales_yoy_pct ?? growth(row.sales_crore, yearAgo?.sales_crore),
      net_profit_yoy_pct: row.net_profit_yoy_pct ?? growth(row.net_profit_crore, yearAgo?.net_profit_crore),
      sales_qoq_pct: row.sales_qoq_pct ?? (prev && prev.key === row.key - 3 ? growth(row.sales_crore, prev.sales_crore) : null),
    };
  });
}

type AnnualRow = {
  period: string;
  sales_crore: number | null;
  expenses_crore: number | null;
  operating_profit_crore: number | null;
  operating_margin_pct: number | null;
  net_profit_crore: number | null;
  eps: number | null;
  dividend_payout_pct: number | null;
};

function sum(values: Array<number | null | undefined>) {
  return values.every(num) ? (values as number[]).reduce((a, b) => a + b, 0) : null;
}

/** Indian fiscal years (Apr–Mar) from four filed quarters, plus trailing twelve months. */
function annualFromQuarters(quarters: QuarterRow[]): AnnualRow[] {
  const byFy = new Map<number, QuarterRow[]>();
  for (const q of quarters) {
    const p = parsePeriod(q.period);
    if (!p) continue;
    const fy = p.month >= 4 ? p.year + 1 : p.year;
    byFy.set(fy, [...(byFy.get(fy) ?? []), q]);
  }
  const build = (period: string, qs: QuarterRow[]): AnnualRow => {
    const sales = sum(qs.map((q) => q.sales_crore));
    const op = sum(qs.map((q) => q.operating_profit_crore));
    return {
      period,
      sales_crore: sales,
      expenses_crore: sum(qs.map((q) => q.expenses_crore)),
      operating_profit_crore: op,
      operating_margin_pct: num(sales) && num(op) && sales !== 0 ? (op / sales) * 100 : null,
      net_profit_crore: sum(qs.map((q) => q.net_profit_crore)),
      eps: sum(qs.map((q) => q.eps)),
      dividend_payout_pct: null,
    };
  };
  const rows = [...byFy.entries()]
    .filter(([, qs]) => qs.length === 4)
    .sort(([a], [b]) => a - b)
    .map(([fy, qs]) => build(`Mar ${fy}`, qs));
  const last4 = quarters.slice(-4);
  if (last4.length === 4 && last4[3].key - last4[0].key === 9) {
    rows.push(build("TTM", last4));
  }
  return rows;
}

type RowSpec<T> = {
  label: string;
  value: (item: T) => ReactNode;
  strong?: boolean;
  sub?: boolean;
};

function StatementTable<T extends { period: string }>({ items, rows }: { items: T[]; rows: Array<RowSpec<T>> }) {
  return (
    <div className="rf-table-wrap">
      <table className="rf-table">
        <thead>
          <tr>
            <th />
            {items.map((item) => (
              <th key={item.period}>{item.period}</th>
            ))}
          </tr>
        </thead>
        <tbody>
          {rows.map((row, index) => (
            <tr key={`${index}-${row.label}`} className={row.strong ? "strong" : row.sub ? "sub" : undefined}>
              <th scope="row">{row.label}</th>
              {items.map((item) => (
                <td key={item.period}>{row.value(item)}</td>
              ))}
            </tr>
          ))}
        </tbody>
      </table>
    </div>
  );
}

function Growth({ value }: { value: number | null | undefined }) {
  if (!num(value)) return <span className="rf-muted">—</span>;
  return <span className={value >= 0 ? "rf-up" : "rf-down"}>{fmtSignedPct(value)}</span>;
}

function Empty({ children }: { children: ReactNode }) {
  return <div className="rf-empty">{children}</div>;
}

export function ResearchFundamentals({
  symbol,
  fundamentals,
  loading,
  error,
  summary,
  market,
  peers,
  onSelectSymbol,
  onDetailsMount,
}: ResearchFundamentalsProps) {
  const [tab, setTab] = useState<TabKey>(readTab);
  const [aboutOpen, setAboutOpen] = useState(false);

  useEffect(() => {
    try {
      window.localStorage.setItem(TAB_STORAGE_KEY, tab);
    } catch {
      // storage blocked
    }
  }, [tab]);

  useEffect(() => setAboutOpen(false), [symbol]);

  const data = fundamentals && fundamentals.symbol === symbol ? fundamentals : null;
  const quarters = useMemo(() => prepareQuarters(data?.quarterly_results ?? []), [data]);
  const annualFiled = useMemo(
    () => [...(data?.profit_loss ?? [])].filter((r) => Number.isFinite(periodKey(r.period))).sort((a, b) => periodKey(a.period) - periodKey(b.period)),
    [data],
  );
  const annualDerived = useMemo(() => (annualFiled.length ? [] : annualFromQuarters(quarters)), [annualFiled, quarters]);
  const sortByPeriod = <T extends { period: string }>(items: T[] | undefined) =>
    [...(items ?? [])].sort((a, b) => (periodKey(a.period) || 0) - (periodKey(b.period) || 0));

  const balance = useMemo(() => sortByPeriod(data?.balance_sheet), [data]);
  const cashflow = useMemo(() => sortByPeriod(data?.cash_flow), [data]);
  const ratios = useMemo(() => sortByPeriod(data?.financial_ratios), [data]);
  const shareholding = useMemo(() => sortByPeriod(data?.shareholding_pattern), [data]);
  const updates = data?.recent_updates ?? [];
  const events = data?.upcoming_events ?? [];

  const tabs: Array<{ key: TabKey; label: string }> = [
    { key: "about", label: "About" },
    { key: "quarters", label: "Quarters" },
    { key: "pnl", label: "Profit & Loss" },
    { key: "peers", label: "Peers" },
    ...(balance.length ? [{ key: "balance" as const, label: "Balance Sheet" }] : []),
    ...(cashflow.length ? [{ key: "cashflow" as const, label: "Cash Flow" }] : []),
    ...(ratios.length ? [{ key: "ratios" as const, label: "Ratios" }] : []),
    ...(shareholding.length ? [{ key: "shareholding" as const, label: "Shareholding" }] : []),
    ...(updates.length || events.length ? [{ key: "updates" as const, label: "Updates" }] : []),
    { key: "details", label: "Details" },
  ];
  const activeTab = tabs.some((t) => t.key === tab) ? tab : "about";

  // Key numbers. P/E falls back to market cap over the last four filed
  // quarters' profit, which is standalone — labelled as such.
  const valuation = data?.valuation ?? null;
  const marketCap = valuation?.market_cap_crore ?? summary?.market_cap_crore ?? null;
  const ttmProfit = quarters.length >= 4 ? sum(quarters.slice(-4).map((q) => q.net_profit_crore)) : null;
  const derivedPe = !num(valuation?.pe_ratio) && num(marketCap) && num(ttmProfit) && ttmProfit > 0 ? marketCap / ttmProfit : null;
  const price = summary?.symbol === symbol ? summary?.last_price ?? null : null;
  // pct_from_52w_high is "% below the high" (positive); pct_from_52w_low is "% above the low".
  const high52 = price != null && num(summary?.pct_from_52w_high) && summary!.pct_from_52w_high < 100 ? price / (1 - summary!.pct_from_52w_high / 100) : null;
  const low52 = price != null && num(summary?.pct_from_52w_low) ? price / (1 + summary!.pct_from_52w_low / 100) : null;

  const latest = quarters[quarters.length - 1];
  const yearAgo = latest ? quarters.find((q) => q.key === latest.key - 12) : undefined;
  const good: string[] = [];
  const bad: string[] = [];
  if (latest) {
    if (num(latest.sales_yoy_pct)) {
      if (latest.sales_yoy_pct >= 15) good.push(`Sales up ${latest.sales_yoy_pct.toFixed(0)}% YoY in ${latest.period}`);
      else if (latest.sales_yoy_pct < 0) bad.push(`Sales down ${Math.abs(latest.sales_yoy_pct).toFixed(0)}% YoY in ${latest.period}`);
    }
    if (num(latest.net_profit_yoy_pct)) {
      if (latest.net_profit_yoy_pct >= 20) good.push(`Net profit up ${latest.net_profit_yoy_pct.toFixed(0)}% YoY`);
      else if (latest.net_profit_yoy_pct < 0) bad.push(`Net profit down ${Math.abs(latest.net_profit_yoy_pct).toFixed(0)}% YoY`);
    }
    if (num(latest.operating_margin_pct) && num(yearAgo?.operating_margin_pct)) {
      const diff = latest.operating_margin_pct - yearAgo!.operating_margin_pct;
      if (diff >= 1) good.push(`Operating margin expanding YoY (${yearAgo!.operating_margin_pct.toFixed(1)}% → ${latest.operating_margin_pct.toFixed(1)}%)`);
      else if (diff <= -1) bad.push(`Operating margin contracting YoY (${yearAgo!.operating_margin_pct.toFixed(1)}% → ${latest.operating_margin_pct.toFixed(1)}%)`);
    }
    if (num(latest.net_profit_crore) && latest.net_profit_crore < 0) bad.push(`Loss of ₹${fmtCrore(Math.abs(latest.net_profit_crore))} Cr in ${latest.period}`);
  }
  const roce = valuation?.roce_pct ?? null;
  if (num(roce)) {
    if (roce >= 20) good.push(`High return on capital: ROCE ${roce.toFixed(0)}%`);
    else if (roce < 10) bad.push(`Low return on capital: ROCE ${roce.toFixed(0)}%`);
  }

  const km = data?.key_metrics ?? {};
  const kmNum = (key: string) => (typeof km[key] === "number" ? (km[key] as number) : null);
  const profileAsOf = typeof km.profile_as_of === "string" ? km.profile_as_of : null;
  const ttmSales = quarters.length >= 4 && quarters[quarters.length - 1].key - quarters[quarters.length - 4].key === 9
    ? sum(quarters.slice(-4).map((q) => q.sales_crore))
    : null;
  const ownSummary = summary?.symbol === symbol ? summary : null;
  const keyRows: Array<[string, string, string?]> = [
    ["Market Cap", num(marketCap) ? `₹${fmtNumber(marketCap)} Cr` : "—"],
    ["Current Price", num(price) ? `₹${fmtNumber(price, 2)}` : "—"],
    ["52W High / Low", num(high52) && num(low52) ? `₹${fmtNumber(high52)} / ${fmtNumber(low52)}` : "—"],
    [
      derivedPe != null ? "Stock P/E (TTM)" : "Stock P/E*",
      fmtNumber(valuation?.pe_ratio ?? derivedPe, 1),
      derivedPe != null ? "Market cap over the last four quarters' standalone net profit" : undefined,
    ],
    ["Price / Book*", fmtNumber(kmNum("price_to_book"), 2)],
    ["Book Value*", num(kmNum("book_value")) ? `₹${fmtNumber(kmNum("book_value"), 1)}` : "—"],
    ["EPS (TTM)*", fmtNumber(kmNum("eps_ttm"), 2)],
    ["ROE", fmtPct(valuation?.roe_pct ?? kmNum("roe_pct"))],
    ["ROCE", fmtPct(valuation?.roce_pct)],
    ["ROA*", fmtPct(kmNum("roa_pct"))],
    ["OPM", fmtPct(valuation?.operating_margin_pct)],
    ["Net Margin", fmtPct(valuation?.net_margin_pct)],
    ["Debt / Equity*", num(kmNum("debt_to_equity")) ? fmtNumber(kmNum("debt_to_equity")! / 100, 2) : "—"],
    ["Current Ratio*", fmtNumber(kmNum("current_ratio"), 2)],
    ["Dividend Yield", fmtPct(valuation?.dividend_yield_pct ?? kmNum("dividend_yield_pct"), 2)],
    ["EV / EBITDA*", fmtNumber(kmNum("ev_to_ebitda"), 1)],
    ["Sales (TTM)", num(ttmSales) ? `₹${fmtCrore(ttmSales)} Cr` : "—"],
    ["Net Profit (TTM)", num(ttmProfit) ? `₹${fmtCrore(ttmProfit)} Cr` : "—"],
    ["Sales Growth YoY", fmtSignedPct(latest?.sales_yoy_pct) || "—"],
    ["Profit Growth YoY", fmtSignedPct(latest?.net_profit_yoy_pct) || "—"],
    ["Total Debt*", num(kmNum("total_debt")) ? `₹${fmtCrore(kmNum("total_debt"))} Cr` : "—"],
    ["Cash*", num(kmNum("total_cash")) ? `₹${fmtCrore(kmNum("total_cash"))} Cr` : "—"],
    ["Insider Holding*", fmtPct(kmNum("promoter_holding_pct"))],
    ["Institutional Holding*", fmtPct(kmNum("institution_holding_pct"))],
    ["Beta*", fmtNumber(kmNum("beta"), 2)],
    ["RS Rating", num(ownSummary?.rs_rating) ? fmtNumber(ownSummary!.rs_rating, 0) : "—"],
    ["1Y Return", num(ownSummary?.stock_return_12m) ? fmtSignedPct(ownSummary!.stock_return_12m) : "—"],
    ["Avg Turnover (30D)", num(ownSummary?.avg_rupee_volume_30d_crore) ? `₹${fmtCrore(ownSummary!.avg_rupee_volume_30d_crore)} Cr` : "—"],
    ["Employees*", num(kmNum("employees")) ? fmtNumber(kmNum("employees")) : "—"],
  ];

  const about = data?.about ?? data?.business_summary ?? null;
  const breadcrumb = [data?.sector ?? summary?.sector, data?.sub_sector ?? summary?.sub_sector].filter(Boolean).join(" › ");

  const annual: AnnualRow[] = annualFiled.length
    ? annualFiled.map((r) => ({ ...r, expenses_crore: null }))
    : annualDerived;

  const body = !symbol ? (
    <Empty>Pick a stock to view fundamentals.</Empty>
  ) : loading && !data ? (
    <Empty>Loading fundamentals for {symbol}…</Empty>
  ) : error && !data ? (
    <Empty>{error}</Empty>
  ) : !data ? (
    <Empty>Fundamentals are not available for this stock yet.</Empty>
  ) : activeTab === "about" ? (
    <div className="rf-section">
      {breadcrumb ? <div className="rf-crumb">{breadcrumb}</div> : null}
      <h3 className="rf-h">About</h3>
      {about ? (
        <>
          <p className={aboutOpen ? "rf-about open" : "rf-about"}>{about}</p>
          {about.length > 320 ? (
            <button type="button" className="rf-link" onClick={() => setAboutOpen((v) => !v)}>
              {aboutOpen ? "Show less" : "Read more"}
            </button>
          ) : null}
        </>
      ) : (
        <p className="rf-muted">{data.name} — a business description is not available from the current sources.</p>
      )}
      {data.company_website ? (
        <a className="rf-link" href={data.company_website} target="_blank" rel="noreferrer">
          {data.company_website.replace(/^https?:\/\//, "").replace(/\/$/, "")}
        </a>
      ) : null}

      <dl className="rf-keys">
        {keyRows.map(([label, value, title]) => (
          <div key={label} title={title}>
            <dt>{label}</dt>
            <dd>{value}</dd>
          </div>
        ))}
      </dl>
      <p className="rf-note">
        Quarterly figures are standalone, from BSE filings.
        {profileAsOf ? ` Ratios marked * are from Yahoo Finance (consolidated), as of ${profileAsOf}.` : ""}
      </p>

      {good.length || bad.length ? (
        <div className="rf-goodbad">
          <div>
            <h4 className="rf-good">The good</h4>
            {good.length ? <ul>{good.map((g) => <li key={g}>{g}</li>)}</ul> : <p className="rf-muted">Nothing stands out.</p>}
          </div>
          <div>
            <h4 className="rf-bad">The bad</h4>
            {bad.length ? <ul>{bad.map((b) => <li key={b}>{b}</li>)}</ul> : <p className="rf-muted">Nothing stands out.</p>}
          </div>
        </div>
      ) : null}
      <p className="rf-note">The good / bad are read off the filed numbers above, not opinions.</p>
    </div>
  ) : activeTab === "quarters" ? (
    quarters.length ? (
      <div className="rf-section">
        <h3 className="rf-h">Quarterly Results <span className="rf-h-note">Latest first · Standalone · ₹ Crores</span></h3>
        <StatementTable
          items={quarters.slice(-8).reverse()}
          rows={[
            { label: "Sales", value: (q) => fmtCrore(q.sales_crore) },
            { label: "YoY", sub: true, value: (q) => <Growth value={q.sales_yoy_pct} /> },
            { label: "Expenses", value: (q) => fmtCrore(q.expenses_crore) },
            { label: "Operating Profit", strong: true, value: (q) => fmtCrore(q.operating_profit_crore) },
            { label: "OPM %", value: (q) => fmtPct(q.operating_margin_pct, 0) },
            { label: "Profit before tax", value: (q) => fmtCrore(q.profit_before_tax_crore) },
            { label: "Net Profit", strong: true, value: (q) => fmtCrore(q.net_profit_crore) },
            { label: "YoY", sub: true, value: (q) => <Growth value={q.net_profit_yoy_pct} /> },
            { label: "EPS in Rs", value: (q) => fmtNumber(q.eps, 2) },
            {
              label: "Raw PDF",
              value: (q) =>
                q.result_document_url ? (
                  <a className="rf-link" href={q.result_document_url} target="_blank" rel="noreferrer" aria-label={`${q.period} result filing`}>
                    ↗
                  </a>
                ) : (
                  ""
                ),
            },
          ]}
        />
      </div>
    ) : (
      <Empty>No quarterly results on file for {symbol}.</Empty>
    )
  ) : activeTab === "pnl" ? (
    annual.length ? (
      <div className="rf-section">
        <h3 className="rf-h">Profit &amp; Loss <span className="rf-h-note">₹ Crores</span></h3>
        {annualFiled.length ? null : (
          <p className="rf-note">Latest first. Fiscal years summed from four filed standalone quarters; TTM is the last four quarters.</p>
        )}
        <StatementTable<AnnualRow>
          items={[...annual].reverse()}
          rows={[
            { label: "Sales", value: (r) => fmtCrore(r.sales_crore) },
            ...(annualFiled.length ? [] : [{ label: "Expenses", value: (r: AnnualRow) => fmtCrore(r.expenses_crore) }]),
            { label: "Operating Profit", strong: true, value: (r) => fmtCrore(r.operating_profit_crore) },
            { label: "OPM %", value: (r) => fmtPct(r.operating_margin_pct, 0) },
            { label: "Net Profit", strong: true, value: (r) => fmtCrore(r.net_profit_crore) },
            { label: "EPS in Rs", value: (r) => fmtNumber(r.eps, 2) },
            ...(annualFiled.length ? [{ label: "Dividend Payout %", value: (r: AnnualRow) => fmtPct(r.dividend_payout_pct, 0) }] : []),
          ]}
        />
      </div>
    ) : (
      <Empty>No annual figures yet — fewer than four quarters of a fiscal year are on file.</Empty>
    )
  ) : activeTab === "peers" ? (
    <PeersTable symbol={symbol} market={market} peers={peers} onSelectSymbol={onSelectSymbol} />
  ) : activeTab === "balance" ? (
    <div className="rf-section">
      <h3 className="rf-h">Balance Sheet <span className="rf-h-note">₹ Crores</span></h3>
      <StatementTable
        items={balance}
        rows={[
          { label: "Shareholders' Equity", value: (r) => fmtCrore(r.shareholders_equity_crore) },
          { label: "Borrowings", value: (r) => fmtCrore(r.debt_crore) },
          { label: "Total Liabilities", value: (r) => fmtCrore(r.total_liabilities_crore) },
          { label: "Current Liabilities", sub: true, value: (r) => fmtCrore(r.current_liabilities_crore) },
          { label: "Total Assets", strong: true, value: (r) => fmtCrore(r.total_assets_crore) },
          { label: "Current Assets", sub: true, value: (r) => fmtCrore(r.current_assets_crore) },
          { label: "Cash & Equivalents", value: (r) => fmtCrore(r.cash_and_equivalents_crore) },
          { label: "Inventory", value: (r) => fmtCrore(r.inventory_crore) },
          { label: "Receivables", value: (r) => fmtCrore(r.receivables_crore) },
        ]}
      />
    </div>
  ) : activeTab === "cashflow" ? (
    <div className="rf-section">
      <h3 className="rf-h">Cash Flow <span className="rf-h-note">₹ Crores</span></h3>
      <StatementTable
        items={cashflow}
        rows={[
          { label: "Operating Activity", value: (r) => fmtCrore(r.operating_cash_flow_crore) },
          { label: "Investing Activity", value: (r) => fmtCrore(r.investing_cash_flow_crore) },
          { label: "Financing Activity", value: (r) => fmtCrore(r.financing_cash_flow_crore) },
          { label: "Capex", value: (r) => fmtCrore(r.capital_expenditure_crore) },
          { label: "Free Cash Flow", strong: true, value: (r) => fmtCrore(r.free_cash_flow_crore) },
          { label: "Dividends Paid", value: (r) => fmtCrore(r.dividends_paid_crore) },
        ]}
      />
    </div>
  ) : activeTab === "ratios" ? (
    <div className="rf-section">
      <h3 className="rf-h">Ratios</h3>
      <StatementTable
        items={ratios}
        rows={[
          { label: "ROCE %", value: (r) => fmtPct(r.roce_pct, 0) },
          { label: "ROE %", value: (r) => fmtPct(r.roe_pct, 0) },
          { label: "ROA %", value: (r) => fmtPct(r.roa_pct, 0) },
          { label: "Debt / Equity", value: (r) => fmtNumber(r.debt_to_equity_ratio, 2) },
          { label: "Current Ratio", value: (r) => fmtNumber(r.current_ratio, 2) },
          { label: "Interest Coverage", value: (r) => fmtNumber(r.interest_coverage, 1) },
          { label: "Asset Turnover", value: (r) => fmtNumber(r.asset_turnover, 2) },
        ]}
      />
    </div>
  ) : activeTab === "shareholding" ? (
    <div className="rf-section">
      <h3 className="rf-h">Shareholding Pattern <span className="rf-h-note">% of shares</span></h3>
      <StatementTable
        items={shareholding.slice(-8)}
        rows={[
          { label: "Promoters", value: (r) => fmtPct(r.promoter_pct, 2) },
          { label: "FIIs", value: (r) => fmtPct(r.fii_pct, 2) },
          { label: "DIIs", value: (r) => fmtPct(r.dii_pct, 2) },
          { label: "Public", value: (r) => fmtPct(r.public_pct, 2) },
          { label: "No. of Shareholders", value: (r) => fmtNumber(r.shareholder_count) },
        ]}
      />
    </div>
  ) : activeTab === "updates" ? (
    <div className="rf-section">
      {events.length ? (
        <>
          <h3 className="rf-h">Upcoming</h3>
          <ul className="rf-list">
            {events.map((e) => (
              <li key={`${e.date}-${e.event}`}><strong>{e.date}</strong> {e.event}</li>
            ))}
          </ul>
        </>
      ) : null}
      {updates.length ? (
        <>
          <h3 className="rf-h">Recent updates</h3>
          <ul className="rf-list">
            {updates.map((u) => (
              <li key={`${u.title}-${u.published_at}`}>
                {u.link ? <a className="rf-link" href={u.link} target="_blank" rel="noreferrer">{u.title}</a> : u.title}
                <span className="rf-muted"> · {u.source}{u.published_at ? ` · ${u.published_at.slice(0, 10)}` : ""}</span>
              </li>
            ))}
          </ul>
        </>
      ) : null}
    </div>
  ) : null;

  return (
    <div className="rf">
      {symbol ? (
        <div className="rf-head">
          <div className="rf-head-name">{data?.name || ownSummary?.name || symbol}</div>
          <div className="rf-head-sub">
            {symbol}
            {breadcrumb ? ` · ${breadcrumb}` : ""}
            {data?.partial ? <span className="rf-head-live"> · loading full data…</span> : null}
          </div>
        </div>
      ) : null}
      <div className="rf-tabs" role="tablist" aria-label="Fundamentals sections">
        {tabs.map((t) => (
          <button
            key={t.key}
            type="button"
            role="tab"
            aria-selected={activeTab === t.key}
            className={activeTab === t.key ? "rf-tab active" : "rf-tab"}
            onClick={() => setTab(t.key)}
          >
            {t.label}
          </button>
        ))}
      </div>
      <div className="rf-body">
        {activeTab === "details" ? null : body}
        {/* The original card view is portalled here by the chart panel, so it
            stays mounted (and costs nothing extra) while another tab shows. */}
        <div ref={onDetailsMount} hidden={activeTab !== "details"} />
      </div>
    </div>
  );
}

type PeerSortKey = "market_cap" | "pe" | "roe" | "opm" | "sales_yoy" | "profit_yoy" | "return_1y" | "rs";

function PeersTable({
  symbol,
  market,
  peers,
  onSelectSymbol,
}: {
  symbol: string;
  market: MarketKey;
  peers: { groupName: string; members: IndustryGroupStockItem[] } | "unavailable" | null;
  onSelectSymbol?: (symbol: string) => void;
}) {
  const [metrics, setMetrics] = useState<Record<string, PeerMetricsItem>>({});
  const [failed, setFailed] = useState(false);
  const [sortKey, setSortKey] = useState<PeerSortKey>("market_cap");

  const members = useMemo(() => {
    const all = [...(peers && peers !== "unavailable" ? peers.members : [])].sort(
      (a, b) => (b.market_cap_cr ?? 0) - (a.market_cap_cr ?? 0),
    );
    const top = all.slice(0, 40);
    // A small company must still appear in its own peer table.
    const self = all.find((m) => m.symbol === symbol);
    return self && !top.includes(self) ? [...top, self] : top;
  }, [peers, symbol]);
  const memberKey = members.map((m) => m.symbol).join(",");

  useEffect(() => {
    if (!memberKey) return;
    let active = true;
    setFailed(false);
    getPeerMetrics(memberKey.split(","), market)
      .then((payload) => {
        if (!active) return;
        setMetrics(Object.fromEntries((payload?.items ?? []).map((item) => [item.symbol, item])));
      })
      .catch(() => active && setFailed(true));
    return () => {
      active = false;
    };
  }, [memberKey, market]);

  if (peers === "unavailable") return <Empty>Industry group data is not available for this stock right now.</Empty>;
  if (!peers) return <Empty>Loading the industry group…</Empty>;
  if (members.length <= 1) return <Empty>No other listed companies in {peers.groupName}.</Empty>;

  const value = (m: IndustryGroupStockItem, key: PeerSortKey): number | null => {
    const x = metrics[m.symbol];
    switch (key) {
      case "market_cap": return m.market_cap_cr ?? null;
      case "pe": return x?.pe ?? null;
      case "roe": return x?.roe_pct ?? null;
      case "opm": return x?.operating_margin_pct ?? null;
      case "sales_yoy": return x?.sales_yoy_pct ?? null;
      case "profit_yoy": return x?.profit_yoy_pct ?? null;
      case "return_1y": return m.return_1y ?? null;
      case "rs": return m.rs_rating ?? null;
    }
  };
  const ascending = sortKey === "pe";
  const rows = [...members].sort((a, b) => {
    const va = value(a, sortKey);
    const vb = value(b, sortKey);
    if (va == null && vb == null) return 0;
    if (va == null) return 1;
    if (vb == null) return -1;
    return ascending ? va - vb : vb - va;
  });
  const median = (key: PeerSortKey) => {
    const values = members.map((m) => value(m, key)).filter(num).sort((a, b) => a - b);
    if (!values.length) return null;
    const mid = Math.floor(values.length / 2);
    return values.length % 2 ? values[mid] : (values[mid - 1] + values[mid]) / 2;
  };
  const cols: Array<{ key: PeerSortKey; label: string; render: (m: IndustryGroupStockItem) => ReactNode; med: (v: number | null) => string }> = [
    { key: "market_cap", label: "Mcap ₹Cr", render: (m) => fmtNumber(m.market_cap_cr), med: (v) => fmtNumber(v) },
    { key: "pe", label: "P/E", render: (m) => fmtNumber(metrics[m.symbol]?.pe, 1), med: (v) => fmtNumber(v, 1) },
    { key: "roe", label: "ROE %", render: (m) => fmtPct(metrics[m.symbol]?.roe_pct, 0), med: (v) => fmtPct(v, 0) },
    { key: "opm", label: "OPM %", render: (m) => fmtPct(metrics[m.symbol]?.operating_margin_pct, 0), med: (v) => fmtPct(v, 0) },
    { key: "sales_yoy", label: "Sales YoY", render: (m) => <Growth value={metrics[m.symbol]?.sales_yoy_pct} />, med: (v) => fmtSignedPct(v) || "—" },
    { key: "profit_yoy", label: "Profit YoY", render: (m) => <Growth value={metrics[m.symbol]?.profit_yoy_pct} />, med: (v) => fmtSignedPct(v) || "—" },
    { key: "return_1y", label: "1Y", render: (m) => <Growth value={m.return_1y} />, med: (v) => fmtSignedPct(v) || "—" },
    { key: "rs", label: "RS", render: (m) => fmtNumber(m.rs_rating, 0), med: (v) => fmtNumber(v, 0) },
  ];

  return (
    <div className="rf-section">
      <h3 className="rf-h">
        Peers <span className="rf-h-note">{peers.groupName} · {members.length} companies · click a column to sort</span>
      </h3>
      {failed ? <p className="rf-note">Ratios could not be loaded; price data is shown.</p> : null}
      <div className="rf-table-wrap">
        <table className="rf-table rf-peers">
          <thead>
            <tr>
              <th className="rf-peer-name">Company</th>
              <th>CMP</th>
              {cols.map((c) => (
                <th key={c.key}>
                  <button type="button" className={sortKey === c.key ? "rf-sort active" : "rf-sort"} onClick={() => setSortKey(c.key)}>
                    {c.label}
                  </button>
                </th>
              ))}
            </tr>
          </thead>
          <tbody>
            {rows.map((m) => (
              <tr key={m.symbol} className={m.symbol === symbol ? "rf-peer-self" : undefined}>
                <th scope="row" className="rf-peer-name">
                  <button type="button" className="rf-link" onClick={() => onSelectSymbol?.(m.symbol)} title={m.symbol}>
                    {m.company_name || m.symbol}
                  </button>
                </th>
                <td>{fmtNumber(m.last_price, m.last_price < 100 ? 2 : 1)}</td>
                {cols.map((c) => (
                  <td key={c.key}>{c.render(m)}</td>
                ))}
              </tr>
            ))}
            <tr className="strong">
              <th scope="row" className="rf-peer-name">Median</th>
              <td />
              {cols.map((c) => (
                <td key={c.key}>{c.med(median(c.key))}</td>
              ))}
            </tr>
          </tbody>
        </table>
      </div>
      <p className="rf-note">P/E and ROE from Yahoo Finance; OPM and growth from the latest standalone BSE quarter.</p>
    </div>
  );
}
