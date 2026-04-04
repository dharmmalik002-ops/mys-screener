import { useEffect, useMemo, useRef, useState } from "react";
import {
  askMoneyFlowCompanyQuestion,
  generateMoneyFlow,
  generateMoneyFlowStocks,
  getMoneyFlowHistory,
  getMoneyFlowStockHistory,
  getSectorRotation,
  type CompanyQuestionResponse,
  type MarketKey,
  type MoneyFlowReport,
  type MoneyFlowSector,
  type MoneyFlowStockIdea,
  type MoneyFlowStockIdeasResponse,
  type SectorRotationItem,
  type SectorRotationStock,
} from "../lib/api";

type Props = {
  market: MarketKey;
  onPickSymbol: (symbol: string) => void;
  onPickSymbolWithContext?: (symbol: string, contextSymbols: string[]) => void;
};

const USD_TO_INR = 83;
const MONEY_FLOW_CACHE_KEY = "mr-malik-money-flow-cache:v1";
const MONEY_FLOW_CACHE_MAX_AGE_MS = 24 * 60 * 60 * 1000;

type PersistedMoneyFlowCache = {
  saved_at: string;
  reports: MoneyFlowReport[];
  stockIdeaReports: MoneyFlowStockIdeasResponse[];
  rotationRows: SectorRotationItem[];
};

function moneyFlowCacheKey(market: MarketKey) {
  return `${MONEY_FLOW_CACHE_KEY}:${market}`;
}

function readMoneyFlowCache(market: MarketKey): PersistedMoneyFlowCache | null {
  if (typeof window === "undefined") {
    return null;
  }

  const raw = window.localStorage.getItem(moneyFlowCacheKey(market));
  if (!raw) {
    return null;
  }

  try {
    const parsed = JSON.parse(raw) as PersistedMoneyFlowCache;
    const ageMs = Date.now() - new Date(parsed.saved_at).getTime();
    if (!Number.isFinite(ageMs) || ageMs > MONEY_FLOW_CACHE_MAX_AGE_MS) {
      return null;
    }
    return parsed;
  } catch {
    return null;
  }
}

function persistMoneyFlowCache(
  market: MarketKey,
  reports: MoneyFlowReport[],
  stockIdeaReports: MoneyFlowStockIdeasResponse[],
  rotationRows: SectorRotationItem[],
) {
  if (typeof window === "undefined") {
    return;
  }

  try {
    window.localStorage.setItem(
      moneyFlowCacheKey(market),
      JSON.stringify({
        saved_at: new Date().toISOString(),
        reports,
        stockIdeaReports,
        rotationRows,
      } satisfies PersistedMoneyFlowCache),
    );
  } catch {
    // Ignore cache persistence failures so live data loading keeps working.
  }
}

function numberLocaleForMarket(market: MarketKey) {
  return market === "us" ? "en-US" : "en-IN";
}

function marketTimeZone(market: MarketKey) {
  return market === "us" ? "America/New_York" : "Asia/Kolkata";
}

function marketTimeZoneLabel(market: MarketKey) {
  return market === "us" ? "ET" : "IST";
}

function benchmarkLabel(market: MarketKey) {
  return market === "us" ? "S&P 500" : "Nifty";
}

function weeklyUpdateDescription(market: MarketKey) {
  return market === "us" ? "Saturday at 9:00 AM ET" : "Saturday at 9:00 AM IST";
}

function dailyUpdateDescription(market: MarketKey) {
  return market === "us" ? "after the US market close" : "6:00 PM IST";
}

function weekLabel(report: MoneyFlowReport, market: MarketKey): string {
  const d = new Date(report.week_start + "T00:00:00");
  return d.toLocaleDateString(numberLocaleForMarket(market), { day: "numeric", month: "short", year: "numeric" }) + " week";
}

function nextWeeklyUpdate(market: MarketKey): string {
  const now = new Date(new Date().toLocaleString("en-US", { timeZone: marketTimeZone(market) }));
  const dayOfWeek = now.getDay(); // 0=Sun
  const daysUntilSat = dayOfWeek === 6 ? 7 : (6 - dayOfWeek);
  const sat = new Date(now);
  sat.setDate(sat.getDate() + daysUntilSat);
  sat.setHours(9, 0, 0, 0);
  return `${sat.toLocaleDateString(numberLocaleForMarket(market), { weekday: "long", day: "numeric", month: "short", year: "numeric" })} at 9:00 AM ${marketTimeZoneLabel(market)}`;
}

function formatMarketDateTime(dateValue: string, market: MarketKey): string {
  return `${new Date(dateValue).toLocaleString(numberLocaleForMarket(market), { timeZone: marketTimeZone(market) })} ${marketTimeZoneLabel(market)}`;
}

function formatCompactNumber(value: number | null | undefined, market: MarketKey, digits = 2): string {
  if (value == null || Number.isNaN(value)) {
    return "—";
  }
  return new Intl.NumberFormat(numberLocaleForMarket(market), { maximumFractionDigits: digits }).format(value);
}

function formatPercent(value: number | null | undefined, suffix = "%"): string {
  if (value == null || Number.isNaN(value)) {
    return "—";
  }
  return `${value >= 0 ? "+" : ""}${value.toFixed(2)}${suffix}`;
}

function metricLabel(key: string): string {
  return key
    .split("_")
    .map((part) => part.charAt(0).toUpperCase() + part.slice(1))
    .join(" ");
}

function nextDailyUpdate(market: MarketKey): string {
  if (market === "us") {
    return "After the next US market close";
  }
  const now = new Date(new Date().toLocaleString("en-US", { timeZone: marketTimeZone(market) }));
  const next = new Date(now);
  next.setHours(18, 0, 0, 0);
  if (next <= now) {
    next.setDate(next.getDate() + 1);
  }
  return `${next.toLocaleDateString(numberLocaleForMarket(market), { weekday: "long", day: "numeric", month: "short", year: "numeric" })} at 6:00 PM ${marketTimeZoneLabel(market)}`;
}

function stockIdeaLabel(dateValue: string, market: MarketKey): string {
  const date = new Date(`${dateValue}T00:00:00`);
  return date.toLocaleDateString(numberLocaleForMarket(market), { weekday: "short", day: "numeric", month: "short", year: "numeric" });
}

function formatMarketCap(value: number | null | undefined, market: MarketKey) {
  if (value == null || Number.isNaN(value)) {
    return "—";
  }
  if (market === "us") {
    const usdValue = (value * 10_000_000) / USD_TO_INR;
    return new Intl.NumberFormat("en-US", {
      style: "currency",
      currency: "USD",
      notation: "compact",
      maximumFractionDigits: 1,
    }).format(usdValue);
  }
  return `${formatCompactNumber(value, market)} Cr`;
}

function formatPrice(value: number | null | undefined, market: MarketKey) {
  if (value == null || Number.isNaN(value)) {
    return "—";
  }
  return `${market === "us" ? "$" : "₹"}${formatCompactNumber(value, market)}`;
}

function SentimentBadge({ sentiment, magnitude }: { sentiment: MoneyFlowSector["sentiment"]; magnitude: MoneyFlowSector["magnitude"] }) {
  const cls = sentiment === "bullish" ? "mf-badge mf-badge--bull" : sentiment === "bearish" ? "mf-badge mf-badge--bear" : "mf-badge mf-badge--neutral";
  const label = { strong: "●●●", moderate: "●●○", mild: "●○○" }[magnitude] ?? "●";
  return <span className={cls} title={`${sentiment} — ${magnitude}`}>{label} {sentiment}</span>;
}

function SectorCard({ item }: { item: MoneyFlowSector }) {
  return (
    <div className={`mf-sector-card mf-sector-card--${item.sentiment}`}>
      <div className="mf-sector-card-header">
        <strong>{item.name}</strong>
        <SentimentBadge sentiment={item.sentiment} magnitude={item.magnitude} />
      </div>
      <p className="mf-sector-card-reason">{item.reason}</p>
    </div>
  );
}

function Section({ title, icon, items, emptyMsg }: {
  title: string;
  icon: string;
  items: MoneyFlowSector[];
  emptyMsg?: string;
}) {
  return (
    <section className="mf-section">
      <h3 className="mf-section-title">{icon} {title}</h3>
      {items.length === 0 ? (
        <p className="mf-empty">{emptyMsg ?? "No data"}</p>
      ) : (
        <div className="mf-grid">
          {items.map((item, i) => <SectorCard key={`${item.name}-${i}`} item={item} />)}
        </div>
      )}
    </section>
  );
}

type RotationPeriod = "1D" | "1W" | "1M";
type StockSortBy = "rs" | "return";
type MoneyFlowTab = "weekly" | "rotation" | "stocks";

function periodLabel(period: RotationPeriod): string {
  if (period === "1D") return "Daily";
  if (period === "1W") return "Weekly";
  return "Monthly";
}

function periodPct(item: SectorRotationItem, period: RotationPeriod): number {
  if (period === "1D") return item.pct_top_gainers_1d;
  if (period === "1W") return item.pct_top_gainers_1w;
  return item.pct_top_gainers_1m;
}

function periodCount(item: SectorRotationItem, period: RotationPeriod): number {
  if (period === "1D") return item.top_gainers_1d;
  if (period === "1W") return item.top_gainers_1w;
  return item.top_gainers_1m;
}

function periodAvg(item: SectorRotationItem, period: RotationPeriod): number {
  if (period === "1D") return item.avg_return_1d;
  if (period === "1W") return item.avg_return_1w;
  return item.avg_return_1m;
}

function periodRank(item: SectorRotationItem, period: RotationPeriod): number {
  if (period === "1D") return item.rank_1d;
  if (period === "1W") return item.rank_1w;
  return item.rank_1m;
}

export function MoneyFlowPanel({ market, onPickSymbol, onPickSymbolWithContext }: Props) {
  const cachedMoneyFlow = readMoneyFlowCache(market);
  const [reports, setReports] = useState<MoneyFlowReport[]>(() => cachedMoneyFlow?.reports ?? []);
  const [stockIdeaReports, setStockIdeaReports] = useState<MoneyFlowStockIdeasResponse[]>(() => cachedMoneyFlow?.stockIdeaReports ?? []);
  const [loadingReports, setLoadingReports] = useState(() => (cachedMoneyFlow?.reports.length ?? 0) === 0);
  const [loadingRotation, setLoadingRotation] = useState(false);
  const [loadingStocks, setLoadingStocks] = useState(false);
  const [generating, setGenerating] = useState(false);
  const [generatingStocks, setGeneratingStocks] = useState(false);
  const [error, setError] = useState<string | null>(null);
  const [rotationError, setRotationError] = useState<string | null>(null);
  const [stocksError, setStocksError] = useState<string | null>(null);
  const [rotationLoaded, setRotationLoaded] = useState(false);
  const [stocksLoaded, setStocksLoaded] = useState(false);
  const [selectedWeekKey, setSelectedWeekKey] = useState<string | null>(() => cachedMoneyFlow?.reports[0]?.week_key ?? null);
  const [rotationRows, setRotationRows] = useState<SectorRotationItem[]>(() => cachedMoneyFlow?.rotationRows ?? []);
  const [activeTab, setActiveTab] = useState<MoneyFlowTab>("weekly");
  const [selectedSector, setSelectedSector] = useState<string | null>(null);
  const [selectedPeriod, setSelectedPeriod] = useState<RotationPeriod>("1W");
  const [stockSortBy, setStockSortBy] = useState<StockSortBy>("rs");
  const [selectedStockRecommendationDate, setSelectedStockRecommendationDate] = useState<string | null>(
    () => cachedMoneyFlow?.stockIdeaReports[0]?.recommendation_date ?? null,
  );
  const [selectedIdeaSymbol, setSelectedIdeaSymbol] = useState<string | null>(null);
  const [companyQuestion, setCompanyQuestion] = useState("");
  const [companyAnswer, setCompanyAnswer] = useState<CompanyQuestionResponse | null>(null);
  const [companyQuestionError, setCompanyQuestionError] = useState<string | null>(null);
  const [askingCompanyQuestion, setAskingCompanyQuestion] = useState(false);
  const stocksSectionRef = useRef<HTMLElement | null>(null);

  // Group week keys by year+month for the selector
  const grouped = useMemo(() => {
    const map = new Map<string, MoneyFlowReport[]>();
    for (const r of reports) {
      const d = new Date(r.week_start + "T00:00:00");
      const grp = d.toLocaleDateString(numberLocaleForMarket(market), { month: "long", year: "numeric" });
      if (!map.has(grp)) map.set(grp, []);
      map.get(grp)!.push(r);
    }
    return map;
  }, [market, reports]);

  const activeReport = useMemo(
    () => reports.find((r) => r.week_key === selectedWeekKey) ?? reports[0] ?? null,
    [reports, selectedWeekKey],
  );

  const sortedDaily = useMemo(
    () => [...rotationRows].sort((a, b) => a.rank_1d - b.rank_1d),
    [rotationRows],
  );
  const sortedWeekly = useMemo(
    () => [...rotationRows].sort((a, b) => a.rank_1w - b.rank_1w),
    [rotationRows],
  );
  const sortedMonthly = useMemo(
    () => [...rotationRows].sort((a, b) => a.rank_1m - b.rank_1m),
    [rotationRows],
  );

  const topDaily = sortedDaily[0] ?? null;
  const topWeekly = sortedWeekly[0] ?? null;
  const topMonthly = sortedMonthly[0] ?? null;

  const selectedSectorRow = useMemo(
    () => (selectedSector ? rotationRows.find((row) => row.sector === selectedSector) ?? null : null),
    [rotationRows, selectedSector],
  );

  const activeStockIdeas = useMemo(
    () => stockIdeaReports.find((report) => report.recommendation_date === selectedStockRecommendationDate) ?? stockIdeaReports[0] ?? null,
    [stockIdeaReports, selectedStockRecommendationDate],
  );

  const allIdeas = useMemo(() => {
    if (!activeStockIdeas) {
      return [] as MoneyFlowStockIdea[];
    }
    return [...activeStockIdeas.consolidating_ideas, ...activeStockIdeas.value_ideas];
  }, [activeStockIdeas]);

  const recommendationSymbols = useMemo(
    () => Array.from(new Set(allIdeas.map((idea) => idea.symbol))),
    [allIdeas],
  );

  const selectedIdea = useMemo(
    () => allIdeas.find((idea) => idea.symbol === selectedIdeaSymbol) ?? allIdeas[0] ?? null,
    [allIdeas, selectedIdeaSymbol],
  );

  const sortedSectorStocks = useMemo(() => {
    if (!selectedSectorRow) return [];
    const stocks = [...selectedSectorRow.stocks];
    if (stockSortBy === "rs") {
      return stocks.sort((a, b) => b.rs_rating - a.rs_rating);
    }
    if (selectedPeriod === "1D") return stocks.sort((a, b) => b.return_1d - a.return_1d);
    if (selectedPeriod === "1W") return stocks.sort((a, b) => b.return_1w - a.return_1w);
    return stocks.sort((a, b) => b.return_1m - a.return_1m);
  }, [selectedSectorRow, selectedPeriod, stockSortBy]);

  function stockReturn(stock: SectorRotationStock, period: RotationPeriod): number {
    if (period === "1D") return stock.return_1d;
    if (period === "1W") return stock.return_1w;
    return stock.return_1m;
  }

  function handlePickSector(sector: string, period: RotationPeriod) {
    setSelectedSector(sector);
    setSelectedPeriod(period);
  }

  useEffect(() => {
    if (!selectedSectorRow || activeTab !== "rotation") {
      return;
    }
    stocksSectionRef.current?.scrollIntoView({ behavior: "smooth", block: "start" });
  }, [activeTab, selectedSectorRow]);

  useEffect(() => {
    if (allIdeas.length === 0) {
      if (selectedIdeaSymbol !== null) {
        setSelectedIdeaSymbol(null);
      }
      return;
    }
    if (!selectedIdeaSymbol || !allIdeas.some((idea) => idea.symbol === selectedIdeaSymbol)) {
      setSelectedIdeaSymbol(allIdeas[0].symbol);
    }
  }, [allIdeas, selectedIdeaSymbol]);

  useEffect(() => {
    setCompanyAnswer(null);
    setCompanyQuestionError(null);
    setCompanyQuestion("");
  }, [selectedIdeaSymbol]);

  useEffect(() => {
    const cached = readMoneyFlowCache(market);
    setReports(cached?.reports ?? []);
    setStockIdeaReports(cached?.stockIdeaReports ?? []);
    setRotationRows(cached?.rotationRows ?? []);
    setLoadingReports((cached?.reports.length ?? 0) === 0);
    setRotationLoaded(false);
    setStocksLoaded(false);
    setSelectedWeekKey(cached?.reports[0]?.week_key ?? null);
    setSelectedStockRecommendationDate(cached?.stockIdeaReports[0]?.recommendation_date ?? null);
    setError(null);
    setRotationError(null);
    setStocksError(null);
  }, [market]);

  useEffect(() => {
    if (reports.length === 0 && stockIdeaReports.length === 0 && rotationRows.length === 0) {
      return;
    }
    persistMoneyFlowCache(market, reports, stockIdeaReports, rotationRows);
  }, [market, reports, stockIdeaReports, rotationRows]);

  async function loadReports() {
    const hasCachedReports = reports.length > 0;
    if (!hasCachedReports) {
      setLoadingReports(true);
    }
    try {
      const res = await getMoneyFlowHistory(market);
      setReports(res.reports);
      setError(null);
      if (res.latest_week_key) {
        setSelectedWeekKey(res.latest_week_key);
      } else if (res.reports.length > 0) {
        setSelectedWeekKey(res.reports[0].week_key);
      }
    } catch (err) {
      if (!hasCachedReports) {
        setError(err instanceof Error ? err.message : "Failed to load");
      }
    } finally {
      setLoadingReports(false);
    }
  }

  async function loadRotation() {
    const hasCachedRotation = rotationRows.length > 0;
    if (!hasCachedRotation) {
      setLoadingRotation(true);
    }
    try {
      const res = await getSectorRotation(market);
      setRotationRows(res.sectors);
      setRotationError(null);
    } catch (err) {
      if (!hasCachedRotation) {
        setRotationError(err instanceof Error ? err.message : "Failed to load sector rotation");
      }
    } finally {
      setRotationLoaded(true);
      setLoadingRotation(false);
    }
  }

  async function loadStocks() {
    const hasCachedStocks = stockIdeaReports.length > 0;
    if (!hasCachedStocks) {
      setLoadingStocks(true);
    }
    try {
      const res = await getMoneyFlowStockHistory(market);
      setStockIdeaReports(res.reports);
      if (res.latest_recommendation_date) {
        setSelectedStockRecommendationDate(res.latest_recommendation_date);
      } else if (res.reports.length > 0) {
        setSelectedStockRecommendationDate(res.reports[0].recommendation_date);
      } else {
        setSelectedStockRecommendationDate(null);
      }
      setStocksError(null);
    } catch (err) {
      if (!hasCachedStocks) {
        setStocksError(err instanceof Error ? err.message : "Failed to load stock recommendations");
      }
    } finally {
      setStocksLoaded(true);
      setLoadingStocks(false);
    }
  }

  useEffect(() => {
    void loadReports();
  }, [market]);

  useEffect(() => {
    if (activeTab !== "rotation" || rotationLoaded || loadingRotation) {
      return;
    }
    void loadRotation();
  }, [activeTab, loadingRotation, rotationLoaded]);

  useEffect(() => {
    if (activeTab !== "stocks" || stocksLoaded || loadingStocks) {
      return;
    }
    void loadStocks();
  }, [activeTab, loadingStocks, stocksLoaded]);

  async function handleGenerate() {
    setGenerating(true);
    setError(null);
    try {
      const report = await generateMoneyFlow(market);
      setReports((prev) => {
        const without = prev.filter((r) => r.week_key !== report.week_key);
        return [report, ...without];
      });
      setSelectedWeekKey(report.week_key);
      // Refresh sector rotation after a new report generation cycle
      void loadRotation();
    } catch (err) {
      setError(err instanceof Error ? err.message : "Generation failed");
    } finally {
      setGenerating(false);
    }
  }

  async function handleGenerateStocks() {
    setGeneratingStocks(true);
    setStocksError(null);
    try {
      const response = await generateMoneyFlowStocks(market);
      setStockIdeaReports((prev) => [response, ...prev.filter((item) => item.recommendation_date !== response.recommendation_date)]);
      setSelectedStockRecommendationDate(response.recommendation_date);
    } catch (err) {
      setStocksError(err instanceof Error ? err.message : "Failed to generate stock recommendations");
    } finally {
      setGeneratingStocks(false);
    }
  }

  async function handleAskCompanyQuestion() {
    if (!selectedIdea) {
      return;
    }
    const question = companyQuestion.trim();
    if (!question) {
      setCompanyQuestionError("Enter a question about the selected company.");
      return;
    }
    setAskingCompanyQuestion(true);
    setCompanyQuestionError(null);
    try {
      const response = await askMoneyFlowCompanyQuestion(selectedIdea.symbol, question, market);
      setCompanyAnswer(response);
    } catch (err) {
      setCompanyQuestionError(err instanceof Error ? err.message : "Failed to ask company question");
    } finally {
      setAskingCompanyQuestion(false);
    }
  }

  const headerGeneratedAt = activeTab === "stocks"
    ? activeStockIdeas?.generated_at ?? null
    : activeReport?.generated_at ?? null;
  const headerNextUpdate = activeTab === "stocks"
    ? activeStockIdeas?.next_update_at ? formatMarketDateTime(activeStockIdeas.next_update_at, market) : nextDailyUpdate(market)
    : nextWeeklyUpdate(market);
  const headerButtonLabel = activeTab === "stocks"
    ? (generatingStocks ? "Generating Stocks…" : "Generate Stocks")
    : (generating ? "Generating…" : "Generate Now");
  const headerButtonDisabled = activeTab === "stocks" ? generatingStocks : generating;
  const handleHeaderGenerate = activeTab === "stocks" ? handleGenerateStocks : handleGenerate;

  if (loadingReports && reports.length === 0) {
    return (
      <div className="mf-loading">
        <div className="spinner" />
        <p>Loading money flow dashboard…</p>
      </div>
    );
  }

  return (
    <div className="mf-layout">
      {/* Header */}
      <header className="mf-header">
        <div className="mf-header-left">
          <h2 className="mf-title">Money Flow</h2>
          <p className="mf-subtitle">
            {activeTab === "stocks"
              ? "AI stock recommendations with thesis, valuation context, and company Q&A"
              : "AI-powered capital flow and sector leadership tracking"}
          </p>
          {headerGeneratedAt && (
            <span className="mf-generated-at">
              Last generated: {formatMarketDateTime(headerGeneratedAt, market)}
            </span>
          )}
        </div>
        <div className="mf-header-right">
          <span className="mf-next-update">Next auto-update: {headerNextUpdate}</span>
          <button
            type="button"
            className={headerButtonDisabled ? "nav-button ghost loading" : "nav-button primary"}
            onClick={handleHeaderGenerate}
            disabled={headerButtonDisabled}
          >
            {headerButtonLabel}
          </button>
        </div>
      </header>

      <div className="mf-mode-tabs" role="tablist" aria-label="Money Flow Views">
        <button
          type="button"
          role="tab"
          aria-selected={activeTab === "weekly"}
          className={activeTab === "weekly" ? "mf-mode-tab active" : "mf-mode-tab"}
          onClick={() => setActiveTab("weekly")}
        >
          Weekly Report
        </button>
        <button
          type="button"
          role="tab"
          aria-selected={activeTab === "rotation"}
          className={activeTab === "rotation" ? "mf-mode-tab active" : "mf-mode-tab"}
          onClick={() => setActiveTab("rotation")}
        >
          Sector Rotation
        </button>
        <button
          type="button"
          role="tab"
          aria-selected={activeTab === "stocks"}
          className={activeTab === "stocks" ? "mf-mode-tab active" : "mf-mode-tab"}
          onClick={() => setActiveTab("stocks")}
        >
          Stocks
        </button>
      </div>

      {activeTab === "weekly" && error && <div className="mf-error">{error}</div>}
      {activeTab === "rotation" && rotationError && <div className="mf-error">{rotationError}</div>}
      {activeTab === "stocks" && stocksError && <div className="mf-error">{stocksError}</div>}

      {activeTab === "weekly" && reports.length === 0 && !error ? (
        <div className="mf-empty-state">
          <p>No reports yet. Click <strong>Generate Now</strong> to create the first analysis.</p>
          <p className="mf-subtitle">Reports are auto-generated every {weeklyUpdateDescription(market)}.</p>
        </div>
      ) : null}

      {activeTab === "weekly" && reports.length > 0 ? (
        <div className="mf-body">
          <aside className="mf-week-sidebar">
            <p className="mf-sidebar-title">Reports</p>
            {[...grouped.entries()].map(([monthYear, groupReports]) => (
              <div key={monthYear} className="mf-week-group">
                <p className="mf-week-group-label">{monthYear}</p>
                {groupReports.map((r) => (
                  <button
                    key={r.week_key}
                    type="button"
                    className={r.week_key === selectedWeekKey ? "mf-week-btn active" : "mf-week-btn"}
                    onClick={() => setSelectedWeekKey(r.week_key)}
                  >
                    {weekLabel(r, market)}
                  </button>
                ))}
              </div>
            ))}
          </aside>

          {activeReport ? (
            <div className="mf-report">
              <div className="mf-report-header">
                <h3 className="mf-report-title">Week of {activeReport.week_start}</h3>
                <span className="mf-week-badge">{activeReport.week_key}</span>
              </div>

              <div className="mf-macro-summary">
                <p className="mf-macro-label">Market Summary</p>
                <p>{activeReport.macro_summary}</p>
              </div>

              <div className="mf-two-col">
                <Section
                  title="Money Inflows"
                  icon="↑"
                  items={activeReport.inflows}
                  emptyMsg="No notable inflows identified"
                />
                <Section
                  title="Money Outflows"
                  icon="↓"
                  items={activeReport.outflows}
                  emptyMsg="No notable outflows identified"
                />
              </div>

              <Section
                title="Sector Performance"
                icon="◈"
                items={activeReport.sector_performance}
                emptyMsg="No sector data"
              />

              <div className="mf-two-col">
                <Section
                  title="Short-Term Headwinds"
                  icon="⚡"
                  items={activeReport.short_term_headwinds}
                  emptyMsg="No headwinds identified"
                />
                <Section
                  title="Short-Term Tailwinds"
                  icon="✦"
                  items={activeReport.short_term_tailwinds}
                  emptyMsg="No tailwinds identified"
                />
              </div>

              <Section
                title="Long-Term Tailwinds"
                icon="⟶"
                items={activeReport.long_term_tailwinds}
                emptyMsg="No long-term themes identified"
              />

              <p className="mf-disclaimer">
                Generated by AI ({activeReport.ai_model}) · Not financial advice · For informational purposes only
              </p>
            </div>
          ) : null}
        </div>
      ) : null}

      {activeTab === "rotation" ? (
        <div className="mf-rotation-wrap">
          {loadingRotation ? (
            <div className="mf-loading">
              <div className="spinner" />
              <p>Loading sector rotation…</p>
            </div>
          ) : null}

          {!loadingRotation && rotationRows.length === 0 ? (
            <div className="mf-empty-state">
              <p>Sector rotation data is not available right now.</p>
            </div>
          ) : null}

          {!loadingRotation && rotationRows.length > 0 ? (
            <>
              <div className="mf-rotation-leaders">
                {[{ key: "1D", item: topDaily }, { key: "1W", item: topWeekly }, { key: "1M", item: topMonthly }].map(({ key, item }) => (
                  <article key={key} className="mf-rotation-leader">
                    <p className="mf-macro-label">{periodLabel(key as RotationPeriod)} Money Leader</p>
                    <h3>{item?.sector ?? "—"}</h3>
                    <p className="mf-rotation-main-value">
                      {item ? `${periodPct(item, key as RotationPeriod).toFixed(1)}%` : "—"}
                    </p>
                    <p className="mf-rotation-sub">
                      {item
                        ? `${periodCount(item, key as RotationPeriod)} of ${item.total_stocks} stocks beat ${benchmarkLabel(market)}`
                        : "No data"}
                    </p>
                  </article>
                ))}
              </div>

              {selectedSectorRow ? (
                <section className="mf-rotation-stocks" ref={stocksSectionRef}>
                  <div className="mf-rotation-stocks-head">
                    <div>
                      <h3 className="mf-section-title">{selectedSectorRow.sector} Stocks</h3>
                      <p className="mf-subtitle">Click any stock below to open its chart popup.</p>
                    </div>
                    <div className="mf-rotation-controls">
                      <label>
                        Period
                        <select
                          value={selectedPeriod}
                          onChange={(event) => setSelectedPeriod(event.target.value as RotationPeriod)}
                        >
                          <option value="1D">1D</option>
                          <option value="1W">1W</option>
                          <option value="1M">1M</option>
                        </select>
                      </label>
                      <label>
                        Sort By
                        <select
                          value={stockSortBy}
                          onChange={(event) => setStockSortBy(event.target.value as StockSortBy)}
                        >
                          <option value="rs">RS Rating</option>
                          <option value="return">Return ({selectedPeriod})</option>
                        </select>
                      </label>
                      <button
                        type="button"
                        className="mf-rotation-close"
                        onClick={() => setSelectedSector(null)}
                        aria-label="Close stock list"
                      >
                        Close
                      </button>
                    </div>
                  </div>

                  <div className="mf-rotation-stock-table">
                    {sortedSectorStocks.map((stock) => (
                      <button
                        key={stock.symbol}
                        type="button"
                        className="mf-rotation-stock-row"
                        onClick={() => {
                          const contextSymbols = sortedSectorStocks.map((item) => item.symbol);
                          if (onPickSymbolWithContext) {
                            onPickSymbolWithContext(stock.symbol, contextSymbols);
                            return;
                          }
                          onPickSymbol(stock.symbol);
                        }}
                      >
                        <div className="mf-rotation-stock-main">
                          <strong>{stock.symbol}</strong>
                          <small>{stock.name}</small>
                        </div>
                        <span className="mf-rotation-stock-rs">RS {stock.rs_rating}</span>
                        <span className="mf-rotation-stock-return">
                          {stockReturn(stock, selectedPeriod) >= 0 ? "+" : ""}
                          {stockReturn(stock, selectedPeriod).toFixed(2)}%
                        </span>
                      </button>
                    ))}
                  </div>
                </section>
              ) : null}

              <div className="mf-rotation-rankings">
                {([
                  { key: "1D", rows: sortedDaily },
                  { key: "1W", rows: sortedWeekly },
                  { key: "1M", rows: sortedMonthly },
                ] as Array<{ key: RotationPeriod; rows: SectorRotationItem[] }>).map(({ key, rows }) => (
                  <section key={key} className="mf-rotation-col">
                    <h3 className="mf-section-title">{periodLabel(key)} Sector Rankings</h3>
                    <div className="mf-rotation-table">
                      {rows.map((row) => (
                        <button
                          key={`${key}-${row.sector}`}
                          type="button"
                          className={selectedSector === row.sector ? "mf-rotation-row active" : "mf-rotation-row"}
                          onClick={() => handlePickSector(row.sector, key)}
                        >
                          <span className="mf-rotation-rank">#{periodRank(row, key)}</span>
                          <div className="mf-rotation-sector">
                            <strong>{row.sector}</strong>
                            <small>
                              {periodCount(row, key)}/{row.total_stocks} beat {benchmarkLabel(market)} · Wtd {periodAvg(row, key) >= 0 ? "+" : ""}{periodAvg(row, key).toFixed(2)}%
                            </small>
                          </div>
                          <span className="mf-rotation-pct">{periodPct(row, key).toFixed(1)}%</span>
                        </button>
                      ))}
                    </div>
                  </section>
                ))}
              </div>
            </>
          ) : null}
        </div>
      ) : null}

      {activeTab === "stocks" ? (
        <div className="mf-body">
          {stockIdeaReports.length > 0 ? (
            <aside className="mf-week-sidebar">
              <p className="mf-sidebar-title">Daily Stock Reports</p>
              {stockIdeaReports.map((report) => (
                <button
                  key={report.recommendation_date}
                  type="button"
                  className={report.recommendation_date === activeStockIdeas?.recommendation_date ? "mf-week-btn active" : "mf-week-btn"}
                  onClick={() => setSelectedStockRecommendationDate(report.recommendation_date)}
                >
                  {stockIdeaLabel(report.recommendation_date, market)}
                </button>
              ))}
            </aside>
          ) : null}

          <div className="mf-stocks-wrap">
          {loadingStocks ? (
            <div className="mf-loading">
              <div className="spinner" />
              <p>Loading stock recommendations…</p>
            </div>
          ) : null}

          {!loadingStocks && stockIdeaReports.length === 0 ? (
            <div className="mf-empty-state">
              <p>No stock recommendations have been generated yet.</p>
              <p className="mf-subtitle">Generate a report manually or wait for the next {dailyUpdateDescription(market)} update.</p>
            </div>
          ) : null}

          {!loadingStocks && activeStockIdeas && allIdeas.length === 0 ? (
            <div className="mf-empty-state">
              <p>No stock ideas were saved for {stockIdeaLabel(activeStockIdeas.recommendation_date, market)}.</p>
              <p className="mf-subtitle">Choose another date from the sidebar or generate a fresh report.</p>
            </div>
          ) : null}

          {!loadingStocks && activeStockIdeas && allIdeas.length > 0 && selectedIdea ? (
            <>
              <div className="mf-report-header">
                <h3 className="mf-report-title">Recommendations for {stockIdeaLabel(activeStockIdeas.recommendation_date, market)}</h3>
                <span className="mf-week-badge">{activeStockIdeas.recommendation_date}</span>
              </div>

              <div className="mf-stocks-grid">
              <div className="mf-stocks-side">
                <section className="mf-stocks-section">
                  <div className="mf-stocks-section-head">
                    <h3 className="mf-section-title">Consolidation Setups</h3>
                    <span className="mf-week-badge">{activeStockIdeas.consolidating_ideas.length}</span>
                  </div>
                  <div className="mf-stocks-list">
                    {activeStockIdeas.consolidating_ideas.map((idea) => (
                      <button
                        key={`con-${idea.symbol}`}
                        type="button"
                        className={selectedIdea.symbol === idea.symbol ? "mf-stock-card active" : "mf-stock-card"}
                        onClick={() => setSelectedIdeaSymbol(idea.symbol)}
                      >
                        <div className="mf-stock-card-top">
                          <div>
                            <strong>{idea.symbol}</strong>
                            <small>{idea.name}</small>
                          </div>
                          <span className="mf-stock-chip">Score {idea.setup_score.toFixed(1)}</span>
                        </div>
                        <p>{idea.setup_summary}</p>
                        <div className="mf-stock-card-meta">
                          <span>{idea.sector}</span>
                          <span>{formatPercent(idea.change_pct)}</span>
                        </div>
                      </button>
                    ))}
                  </div>
                </section>

                <section className="mf-stocks-section">
                  <div className="mf-stocks-section-head">
                    <h3 className="mf-section-title">Cheap Valuation Ideas</h3>
                    <span className="mf-week-badge">{activeStockIdeas.value_ideas.length}</span>
                  </div>
                  <div className="mf-stocks-list">
                    {activeStockIdeas.value_ideas.map((idea) => (
                      <button
                        key={`val-${idea.symbol}`}
                        type="button"
                        className={selectedIdea.symbol === idea.symbol ? "mf-stock-card active" : "mf-stock-card"}
                        onClick={() => setSelectedIdeaSymbol(idea.symbol)}
                      >
                        <div className="mf-stock-card-top">
                          <div>
                            <strong>{idea.symbol}</strong>
                            <small>{idea.name}</small>
                          </div>
                          <span className="mf-stock-chip">Score {idea.setup_score.toFixed(1)}</span>
                        </div>
                        <p>{idea.valuation_summary ?? idea.setup_summary}</p>
                        <div className="mf-stock-card-meta">
                          <span>{idea.sector}</span>
                          <span>{formatMarketCap(idea.market_cap_crore, market)}</span>
                        </div>
                      </button>
                    ))}
                  </div>
                </section>
              </div>

              <section className="mf-stock-detail">
                <div className="mf-stock-detail-head">
                  <div>
                    <p className="mf-macro-label">{selectedIdea.recommendation_type === "value" ? "Value idea" : "Consolidation idea"}</p>
                    <h3 className="mf-stock-detail-title">{selectedIdea.symbol} · {selectedIdea.name}</h3>
                    <p className="mf-subtitle">{selectedIdea.sector} · {selectedIdea.sub_sector} · {selectedIdea.exchange}</p>
                  </div>
                  <button
                    type="button"
                    className="nav-button ghost"
                    onClick={() => {
                      if (onPickSymbolWithContext) {
                        onPickSymbolWithContext(selectedIdea.symbol, recommendationSymbols);
                        return;
                      }
                      onPickSymbol(selectedIdea.symbol);
                    }}
                  >
                    Open Chart
                  </button>
                </div>

                <div className="mf-stock-metrics">
                  <article className="mf-stock-metric-card">
                    <span>Price</span>
                    <strong>{formatPrice(selectedIdea.last_price, market)}</strong>
                    <small>{formatPercent(selectedIdea.change_pct)}</small>
                  </article>
                  <article className="mf-stock-metric-card">
                    <span>Market Cap</span>
                    <strong>{formatMarketCap(selectedIdea.market_cap_crore, market)}</strong>
                    <small>RS {selectedIdea.rs_rating ?? "—"}</small>
                  </article>
                  <article className="mf-stock-metric-card">
                    <span>20D / 60D</span>
                    <strong>{formatPercent(selectedIdea.stock_return_20d)} / {formatPercent(selectedIdea.stock_return_60d)}</strong>
                    <small>12M {formatPercent(selectedIdea.stock_return_12m)}</small>
                  </article>
                  <article className="mf-stock-metric-card">
                    <span>52W High Gap</span>
                    <strong>{formatPercent(selectedIdea.pct_from_52w_high)}</strong>
                    <small>ATH {formatPercent(selectedIdea.pct_from_ath)}</small>
                  </article>
                </div>

                <div className="mf-stock-summary-grid">
                  <section className="mf-stock-summary-card">
                    <p className="mf-macro-label">Setup</p>
                    <h4>{selectedIdea.setup_summary}</h4>
                    <p>{selectedIdea.thesis}</p>
                  </section>
                  <section className="mf-stock-summary-card">
                    <p className="mf-macro-label">Recent Quarter</p>
                    <p>{selectedIdea.recent_quarter_summary}</p>
                  </section>
                  <section className="mf-stock-summary-card">
                    <p className="mf-macro-label">Future Growth</p>
                    <p>{selectedIdea.future_growth_summary}</p>
                  </section>
                  <section className="mf-stock-summary-card">
                    <p className="mf-macro-label">Valuation</p>
                    <p>{selectedIdea.valuation_summary ?? "Valuation is not attractive enough to form a value call yet."}</p>
                  </section>
                </div>

                <div className="mf-stock-columns">
                  <section className="mf-stock-list-card">
                    <h4>Recent Developments</h4>
                    {selectedIdea.recent_developments.length > 0 ? (
                      <ul>
                        {selectedIdea.recent_developments.map((item) => <li key={item}>{item}</li>)}
                      </ul>
                    ) : (
                      <p className="mf-empty">No recent developments captured.</p>
                    )}
                  </section>

                  <section className="mf-stock-list-card">
                    <h4>Growth Drivers</h4>
                    {selectedIdea.growth_drivers.length > 0 ? (
                      <ul>
                        {selectedIdea.growth_drivers.map((item) => <li key={item}>{item}</li>)}
                      </ul>
                    ) : (
                      <p className="mf-empty">No growth drivers captured.</p>
                    )}
                  </section>

                  <section className="mf-stock-list-card">
                    <h4>Risk Flags</h4>
                    {selectedIdea.risk_flags.length > 0 ? (
                      <ul>
                        {selectedIdea.risk_flags.map((item) => <li key={item}>{item}</li>)}
                      </ul>
                    ) : (
                      <p className="mf-empty">No immediate risk flags captured.</p>
                    )}
                  </section>
                </div>

                <section className="mf-stock-list-card">
                  <h4>Key Metrics</h4>
                  <div className="mf-key-metrics-grid">
                    {Object.entries(selectedIdea.key_metrics).map(([key, value]) => (
                      <div key={key} className="mf-key-metric-item">
                        <span>{metricLabel(key)}</span>
                        <strong>{typeof value === "number" ? formatCompactNumber(value, market) : value}</strong>
                      </div>
                    ))}
                  </div>
                </section>

                <section className="mf-stock-qa-card">
                  <div className="mf-stocks-section-head">
                    <h4>Ask About {selectedIdea.symbol}</h4>
                    {activeStockIdeas.ai_model ? <span className="mf-week-badge">AI {activeStockIdeas.ai_model}</span> : null}
                  </div>
                  <textarea
                    className="mf-stock-question-input"
                    value={companyQuestion}
                    onChange={(event) => setCompanyQuestion(event.target.value)}
                    placeholder="Ask about growth, risks, valuation, quarter results, or business outlook"
                    rows={4}
                  />
                  <div className="mf-stock-qa-actions">
                    <button
                      type="button"
                      className={askingCompanyQuestion ? "nav-button ghost loading" : "nav-button primary"}
                      onClick={handleAskCompanyQuestion}
                      disabled={askingCompanyQuestion}
                    >
                      {askingCompanyQuestion ? "Thinking…" : "Ask AI"}
                    </button>
                  </div>
                  {companyQuestionError ? <div className="mf-error mf-inline-error">{companyQuestionError}</div> : null}
                  {companyAnswer ? (
                    <div className="mf-stock-answer">
                      <p className="mf-macro-label">Answer</p>
                      <p>{companyAnswer.answer}</p>
                    </div>
                  ) : null}
                </section>
              </section>
              </div>
            </>
          ) : null}
          </div>
        </div>
      ) : null}
    </div>
  );
}
