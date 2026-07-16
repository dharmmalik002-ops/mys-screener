import { useCallback, useEffect, useMemo, useRef, useState } from "react";
import { createPortal } from "react-dom";
import { BarChart3, CalendarDays, Newspaper } from "lucide-react";
import { getEarningsSummary } from "../lib/api";
import type { MarketKey, QuarterlyResultItem } from "../lib/api";

import "./NewsModal.css";

export type NewsModalProps = {
  isOpen: boolean;
  onClose: () => void;
  title: string;
  symbols: string[];
  market: MarketKey;
  accentColor?: string;
};

type NewsItem = { title: string; link: string; summary: string; date: string; source: string; ts: number };

type GrowthRow = {
  label: string;
  latestValue: string;
  qoqPct: number | null;
  yoyPct: number | null;
};

type SymbolNews = {
  symbol: string;
  upcoming: NewsItem[];
  results: NewsItem[];
  general: NewsItem[];
  resultPeriod: string | null;
  resultAnnouncedDate: string | null;
  growthRows: GrowthRow[];
  loading: boolean;
};

const CORP_ANNOUNCE_PATTERNS = [
  /\bcorporate announcement\b/i,
  /\binsider trading\b/i,
  /\bdisclosures? under regulation\b/i,
  /\bcompliance certificate\b/i,
  /\bpostal ballot\b/i,
  /\bpledge of\b/i,
  /\bsast\b/i,
  /\bregulation 7\(2\)\b/i,
  /\bvotes? cast\b/i,
];

const CORP_SOURCE_PATTERNS = [
  /\bbse\s*india\b/i,
  /\bnse\s*india\b/i,
  /\bbseindia\.com\b/i,
  /\bnseindia\.com\b/i,
];

function isCorporateNoise(title: string, source: string): boolean {
  if (CORP_SOURCE_PATTERNS.some((re) => re.test(source))) return true;
  return CORP_ANNOUNCE_PATTERNS.some((re) => re.test(title));
}

function cleanSummary(html: string): string {
  let s = html.replace(/<[^>]*>/g, "").trim();
  s = s.replace(/&nbsp;/g, " ").replace(/&amp;/g, "&").replace(/&quot;/g, '"').replace(/&#39;/g, "'");
  if (s.length > 220) s = s.slice(0, 220).trim() + "…";
  return s;
}

function parseDate(raw: string | undefined): { display: string; ts: number } {
  if (!raw) return { display: "", ts: 0 };
  const d = new Date(raw.replace(/-/g, "/"));
  if (isNaN(d.getTime())) return { display: raw, ts: 0 };
  return {
    display: d.toLocaleString([], { dateStyle: "medium", timeStyle: "short" }),
    ts: d.getTime(),
  };
}

function formatDateOnly(ts: number): string {
  if (!ts) return "";
  return new Date(ts).toLocaleDateString([], { day: "2-digit", month: "short", year: "numeric" });
}

function fmtPct(v: number | null | undefined): string {
  if (v === null || v === undefined || !isFinite(v)) return "—";
  const sign = v > 0 ? "+" : "";
  return `${sign}${v.toFixed(1)}%`;
}

function pctClass(v: number | null | undefined): string {
  if (v === null || v === undefined || !isFinite(v)) return "neutral";
  if (v > 0.05) return "pos";
  if (v < -0.05) return "neg";
  return "neutral";
}

function fmtCrore(v: number | null | undefined): string {
  if (v === null || v === undefined) return "—";
  if (Math.abs(v) >= 1000) return `₹${(v / 1000).toFixed(2)}K Cr`;
  return `₹${v.toFixed(0)} Cr`;
}

function fmtNum(v: number | null | undefined, digits = 2): string {
  if (v === null || v === undefined || !isFinite(v)) return "—";
  return v.toFixed(digits);
}

function pctChange(latest: number | null | undefined, base: number | null | undefined): number | null {
  if (latest === null || latest === undefined || base === null || base === undefined) return null;
  if (base === 0) return null;
  return ((latest - base) / Math.abs(base)) * 100;
}

function computeGrowth(quarters: QuarterlyResultItem[]): GrowthRow[] {
  const latest = quarters[0];
  const prevQ = quarters[1];
  const yearAgo = quarters[4];
  if (!latest) return [];

  const sales = {
    label: "Sales",
    latestValue: fmtCrore(latest.sales_crore),
    qoqPct: latest.sales_qoq_pct ?? pctChange(latest.sales_crore, prevQ?.sales_crore ?? null),
    yoyPct: latest.sales_yoy_pct ?? pctChange(latest.sales_crore, yearAgo?.sales_crore ?? null),
  };
  const profit = {
    label: "Net Profit",
    latestValue: fmtCrore(latest.net_profit_crore),
    qoqPct: latest.net_profit_qoq_pct ?? pctChange(latest.net_profit_crore, prevQ?.net_profit_crore ?? null),
    yoyPct: latest.net_profit_yoy_pct ?? pctChange(latest.net_profit_crore, yearAgo?.net_profit_crore ?? null),
  };
  const eps = {
    label: "EPS",
    latestValue: latest.eps !== null && latest.eps !== undefined ? `₹${fmtNum(latest.eps)}` : "—",
    qoqPct: latest.eps_qoq_pct ?? pctChange(latest.eps, prevQ?.eps ?? null),
    yoyPct: latest.eps_yoy_pct ?? pctChange(latest.eps, yearAgo?.eps ?? null),
  };
  const margin = {
    label: "Op. Margin",
    latestValue: latest.operating_margin_pct !== null && latest.operating_margin_pct !== undefined
      ? `${fmtNum(latest.operating_margin_pct, 1)}%`
      : "—",
    qoqPct: latest.operating_margin_pct !== null && latest.operating_margin_pct !== undefined && prevQ?.operating_margin_pct !== null && prevQ?.operating_margin_pct !== undefined
      ? latest.operating_margin_pct - prevQ.operating_margin_pct
      : null,
    yoyPct: latest.operating_margin_pct !== null && latest.operating_margin_pct !== undefined && yearAgo?.operating_margin_pct !== null && yearAgo?.operating_margin_pct !== undefined
      ? latest.operating_margin_pct - yearAgo.operating_margin_pct
      : null,
  };
  return [sales, profit, eps, margin];
}

type RawRssItem = { title?: string; link?: string; description?: string; pubDate?: string; author?: string };

async function rssSearch(query: string): Promise<NewsItem[]> {
  const cb = Date.now();
  const feedUrl = `https://news.google.com/rss/search?q=${encodeURIComponent(query)}&hl=en-IN&gl=IN&ceid=IN:en&cb=${cb}`;
  try {
    const res = await fetch(`https://api.rss2json.com/v1/api.json?rss_url=${encodeURIComponent(feedUrl)}`);
    const data = await res.json();
    if (data.status !== "ok" || !data.items?.length) return [];
    return (data.items as RawRssItem[])
      .map((item) => {
        const fullTitle = (item.title || "").trim();
        const sourceMatch = fullTitle.match(/\s+-\s+([^-]+)$/);
        const source = sourceMatch ? sourceMatch[1].trim() : (item.author || "");
        const titleClean = sourceMatch ? fullTitle.replace(/\s+-\s+[^-]+$/, "").trim() : fullTitle;
        const summary = cleanSummary(item.description || "") || "";
        const dateInfo = parseDate(item.pubDate);
        return { title: titleClean, link: item.link || "", summary, date: dateInfo.display, source, ts: dateInfo.ts };
      })
      .filter((x) => x.title && !isCorporateNoise(x.title, x.source))
      .sort((a, b) => b.ts - a.ts);
  } catch {
    return [];
  }
}

const RESULT_KEYWORDS = /\b(results?|earnings|quarter(?:ly)?|q[1-4]|profit|net profit|revenue|topline|bottomline|ebitda|margin|posts?|reports?)\b/i;

const UPCOMING_KEYWORDS = /\b(to announce|to declare|results? date|results? on|board meeting|will announce|expected to announce|likely to|preview|results? preview|q[1-4] results? on|earnings? on)\b/i;

async function fetchSymbolNews(symbol: string): Promise<{ upcoming: NewsItem[]; results: NewsItem[]; general: NewsItem[] }> {
  const company = symbol.replace(/\.(NS|BO)$/i, "");

  const upcomingQuery = `${company} (board meeting OR "results date" OR "to announce" OR preview OR "results on") -site:bseindia.com -site:nseindia.com when:30d`;
  const resultsQuery = `${company} (results OR earnings OR quarterly OR Q1 OR Q2 OR Q3 OR Q4 OR profit OR revenue) -site:bseindia.com -site:nseindia.com when:120d`;
  const generalQuery = `${company} stock OR shares OR company -site:bseindia.com -site:nseindia.com when:14d`;

  const [upcomingRaw, resultsRaw, generalRaw] = await Promise.all([
    rssSearch(upcomingQuery),
    rssSearch(resultsQuery),
    rssSearch(generalQuery),
  ]);

  const upcoming = upcomingRaw.filter((x) => UPCOMING_KEYWORDS.test(x.title)).slice(0, 4);
  const upcomingLinks = new Set(upcoming.map((u) => u.link));

  const results = resultsRaw
    .filter((x) => RESULT_KEYWORDS.test(x.title) && !upcomingLinks.has(x.link))
    .slice(0, 6);
  const resultLinks = new Set(results.map((r) => r.link));

  const general = generalRaw
    .filter((x) => !upcomingLinks.has(x.link) && !resultLinks.has(x.link))
    .slice(0, 6);

  return { upcoming, results, general };
}

async function fetchEarnings(symbol: string, market: MarketKey): Promise<{ period: string | null; growth: GrowthRow[] }> {
  try {
    const r = await getEarningsSummary(symbol, market);
    const quarters = r?.quarterly_results ?? [];
    return {
      period: quarters[0]?.period || null,
      growth: computeGrowth(quarters),
    };
  } catch {
    return { period: null, growth: [] };
  }
}

export function NewsModal({ isOpen, onClose, title, symbols, market, accentColor = "#ff6b6b" }: NewsModalProps) {
  const [rows, setRows] = useState<SymbolNews[]>([]);
  const [loadingAll, setLoadingAll] = useState(false);
  const [refreshKey, setRefreshKey] = useState(0);
  const aborted = useRef(false);
  const lastRunSig = useRef<string>("");

  const symbolList = useMemo(
    () => Array.from(new Set(symbols.map((s) => s.toUpperCase()))).filter(Boolean),
    [symbols],
  );
  const symbolKey = useMemo(() => symbolList.join("|"), [symbolList]);

  const runFetch = useCallback(async () => {
    if (!symbolList.length) {
      setRows([]);
      return;
    }
    aborted.current = false;
    setLoadingAll(true);
    setRows(symbolList.map((s) => ({
      symbol: s,
      upcoming: [],
      results: [],
      general: [],
      resultPeriod: null,
      resultAnnouncedDate: null,
      growthRows: [],
      loading: true,
    })));
    for (let i = 0; i < symbolList.length; i++) {
      if (aborted.current) return;
      const sym = symbolList[i];
      const [news, earn] = await Promise.all([
        fetchSymbolNews(sym),
        fetchEarnings(sym, market),
      ]);
      if (aborted.current) return;
      const announcedTs = news.results[0]?.ts || 0;
      const announcedDate = announcedTs ? formatDateOnly(announcedTs) : null;
      setRows((prev) => prev.map((r) => (r.symbol === sym
        ? {
            ...r,
            ...news,
            resultPeriod: earn.period,
            resultAnnouncedDate: announcedDate,
            growthRows: earn.growth,
            loading: false,
          }
        : r)));
      if (i < symbolList.length - 1) await new Promise((r) => setTimeout(r, 350));
    }
    setLoadingAll(false);
  }, [symbolList, market]);

  // Fetch once per (open + symbolKey + refresh). Do NOT re-fetch on parent re-render.
  useEffect(() => {
    if (!isOpen) return;
    const sig = `${symbolKey}::${refreshKey}`;
    if (lastRunSig.current === sig) return;
    lastRunSig.current = sig;
    void runFetch();
    return () => {
      aborted.current = true;
    };
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [isOpen, symbolKey, refreshKey]);

  // Reset signature when modal closes so reopening triggers a fresh fetch.
  useEffect(() => {
    if (!isOpen) {
      lastRunSig.current = "";
    }
  }, [isOpen]);

  useEffect(() => {
    if (!isOpen) return;
    const onKey = (e: KeyboardEvent) => {
      if (e.key === "Escape") onClose();
    };
    window.addEventListener("keydown", onKey);
    document.body.style.overflow = "hidden";
    return () => {
      window.removeEventListener("keydown", onKey);
      document.body.style.overflow = "";
    };
  }, [isOpen, onClose]);

  if (!isOpen) return null;

  const totalLoaded = rows.filter((r) => !r.loading).length;
  const totalItems = rows.reduce((s, r) => s + r.upcoming.length + r.results.length + r.general.length, 0);

  return createPortal(
    <div className="news-modal-overlay" onClick={(e) => { if ((e.target as HTMLElement).classList.contains("news-modal-overlay")) onClose(); }}>
      <div className="news-modal" style={{ ["--news-accent" as never]: accentColor }}>
        <div className="news-modal-header">
          <div className="news-modal-title-row">
            <span className="news-modal-emoji"><Newspaper size={18} strokeWidth={2.2} aria-hidden="true" /></span>
            <div>
              <div className="news-modal-title">{title}</div>
              <div className="news-modal-sub">
                {loadingAll
                  ? `Fetching… ${totalLoaded}/${symbolList.length} symbols`
                  : `${symbolList.length} symbols · ${totalItems} headlines · Upcoming + Results (120d) + News (14d)`}
              </div>
            </div>
          </div>
          <div className="news-modal-actions">
            <button
              type="button"
              className="news-btn-refresh"
              onClick={() => setRefreshKey((k) => k + 1)}
              disabled={loadingAll}
            >
              {loadingAll ? "Fetching…" : "↻ Refresh"}
            </button>
            <button type="button" className="news-btn-close" onClick={onClose} aria-label="Close news widget">✕</button>
          </div>
        </div>

        <div className="news-modal-body">
          {symbolList.length === 0 && (
            <div className="news-empty">No symbols to fetch news for. Add stocks first.</div>
          )}
          <div className="news-grid">
            {rows.map((row) => {
              const announceLabel = row.resultAnnouncedDate
                ? `Result reported ${row.resultAnnouncedDate}${row.resultPeriod ? ` · ${row.resultPeriod}` : ""}`
                : row.resultPeriod
                  ? `Latest reported ${row.resultPeriod}`
                  : "";
              return (
                <article key={row.symbol} className="news-card">
                  <header className="news-card-head">
                    <div className="news-sym">{row.symbol}</div>
                    {announceLabel ? (
                      <div className="news-result-pill" title="Latest result announcement coverage"><CalendarDays size={12} strokeWidth={2.2} aria-hidden="true" /> {announceLabel}</div>
                    ) : !row.loading ? (
                      <div className="news-result-pill news-result-pill--muted"><CalendarDays size={12} strokeWidth={2.2} aria-hidden="true" /> No result data</div>
                    ) : null}
                  </header>

                  {row.loading && <div className="news-card-loading">Loading…</div>}

                  {!row.loading && row.growthRows.length > 0 && (
                    <div className="news-growth">
                      <div className="news-growth-head">
                        <span>Metric</span>
                        <span>Latest</span>
                        <span>QoQ</span>
                        <span>YoY</span>
                      </div>
                      {row.growthRows.map((g) => (
                        <div key={g.label} className="news-growth-row">
                          <span className="news-growth-label">{g.label}</span>
                          <span className="news-growth-val">{g.latestValue}</span>
                          <span className={`news-growth-pct ${pctClass(g.qoqPct)}`}>{fmtPct(g.qoqPct)}</span>
                          <span className={`news-growth-pct ${pctClass(g.yoyPct)}`}>{fmtPct(g.yoyPct)}</span>
                        </div>
                      ))}
                    </div>
                  )}

                  {!row.loading && (
                    <>
                      {row.upcoming.length > 0 && (
                        <section className="news-section">
                          <div className="news-section-title news-section-upcoming">📆 Upcoming · Result Date</div>
                          <ul className="news-list">
                            {row.upcoming.map((item, i) => (
                              <li key={`u-${i}`} className="news-item">
                                <a href={item.link} target="_blank" rel="noopener noreferrer" className="news-item-title">{item.title}</a>
                                <div className="news-item-meta">
                                  {item.source && <span className="news-item-source">{item.source}</span>}
                                  {item.date && <span className="news-item-date">🕐 {item.date}</span>}
                                </div>
                                {item.summary && <p className="news-item-summary">{item.summary}</p>}
                              </li>
                            ))}
                          </ul>
                        </section>
                      )}

                      <section className="news-section">
                        <div className="news-section-title news-section-results"><BarChart3 size={13} strokeWidth={2.2} aria-hidden="true" /> Latest Results Coverage</div>
                        {row.results.length === 0 ? (
                          <div className="news-card-empty">No result-related coverage in last 120 days.</div>
                        ) : (
                          <ul className="news-list">
                            {row.results.map((item, i) => (
                              <li key={`r-${i}`} className="news-item">
                                <a href={item.link} target="_blank" rel="noopener noreferrer" className="news-item-title">{item.title}</a>
                                <div className="news-item-meta">
                                  {item.source && <span className="news-item-source">{item.source}</span>}
                                  {item.date && <span className="news-item-date">🕐 {item.date}</span>}
                                </div>
                                {item.summary && <p className="news-item-summary">{item.summary}</p>}
                              </li>
                            ))}
                          </ul>
                        )}
                      </section>

                      <section className="news-section">
                        <div className="news-section-title news-section-general"><Newspaper size={13} strokeWidth={2.2} aria-hidden="true" /> Recent News (14d)</div>
                        {row.general.length === 0 ? (
                          <div className="news-card-empty">No news-house coverage in last 14 days.</div>
                        ) : (
                          <ul className="news-list">
                            {row.general.map((item, i) => (
                              <li key={`g-${i}`} className="news-item">
                                <a href={item.link} target="_blank" rel="noopener noreferrer" className="news-item-title">{item.title}</a>
                                <div className="news-item-meta">
                                  {item.source && <span className="news-item-source">{item.source}</span>}
                                  {item.date && <span className="news-item-date">🕐 {item.date}</span>}
                                </div>
                                {item.summary && <p className="news-item-summary">{item.summary}</p>}
                              </li>
                            ))}
                          </ul>
                        )}
                      </section>
                    </>
                  )}
                </article>
              );
            })}
          </div>
        </div>
      </div>
    </div>,
    document.body,
  );
}
