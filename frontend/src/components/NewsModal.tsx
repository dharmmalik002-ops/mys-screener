import { useCallback, useEffect, useMemo, useRef, useState } from "react";
import { createPortal } from "react-dom";
import { getEarningsSummary } from "../lib/api";
import type { MarketKey } from "../lib/api";

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
type SymbolNews = {
  symbol: string;
  results: NewsItem[];
  general: NewsItem[];
  resultPeriod: string | null;
  resultAnnouncedDate: string | null;
  resultAnnouncedTs: number;
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
        const summary = cleanSummary(item.description || "") || "Click to read full article.";
        const dateInfo = parseDate(item.pubDate);
        return { title: titleClean, link: item.link || "", summary, date: dateInfo.display, source, ts: dateInfo.ts };
      })
      .filter((x) => x.title && !isCorporateNoise(x.title, x.source))
      .sort((a, b) => b.ts - a.ts);
  } catch {
    return [];
  }
}

const RESULT_KEYWORDS = /\b(results?|earnings|quarter(?:ly)?|q[1-4]|profit|net profit|revenue|topline|bottomline|ebitda|margin|guidance|reports? q[1-4])\b/i;

async function fetchSymbolNews(symbol: string): Promise<{ results: NewsItem[]; general: NewsItem[] }> {
  const company = symbol.replace(/\.(NS|BO)$/i, "");

  const resultsQuery = `${company} (results OR earnings OR quarterly OR Q1 OR Q2 OR Q3 OR Q4 OR profit) -site:bseindia.com -site:nseindia.com when:90d`;
  const generalQuery = `${company} stock OR shares -site:bseindia.com -site:nseindia.com when:7d`;

  const [resultsRaw, generalRaw] = await Promise.all([rssSearch(resultsQuery), rssSearch(generalQuery)]);

  const results = resultsRaw.filter((x) => RESULT_KEYWORDS.test(x.title)).slice(0, 5);

  const seen = new Set(results.map((r) => r.link));
  const general = generalRaw.filter((x) => !seen.has(x.link)).slice(0, 6);

  return { results, general };
}

async function fetchResultPeriod(symbol: string, market: MarketKey): Promise<string | null> {
  try {
    const r = await getEarningsSummary(symbol, market);
    return r?.quarterly_results?.[0]?.period || null;
  } catch {
    return null;
  }
}

export function NewsModal({ isOpen, onClose, title, symbols, market, accentColor = "#ff6b6b" }: NewsModalProps) {
  const [rows, setRows] = useState<SymbolNews[]>([]);
  const [loadingAll, setLoadingAll] = useState(false);
  const [refreshKey, setRefreshKey] = useState(0);
  const aborted = useRef(false);

  const symbolList = useMemo(() => Array.from(new Set(symbols.map((s) => s.toUpperCase()))).filter(Boolean), [symbols]);

  const runFetch = useCallback(async () => {
    if (!symbolList.length) {
      setRows([]);
      return;
    }
    aborted.current = false;
    setLoadingAll(true);
    setRows(symbolList.map((s) => ({ symbol: s, results: [], general: [], resultPeriod: null, resultAnnouncedDate: null, resultAnnouncedTs: 0, loading: true })));
    for (let i = 0; i < symbolList.length; i++) {
      if (aborted.current) return;
      const sym = symbolList[i];
      const [news, period] = await Promise.all([fetchSymbolNews(sym), fetchResultPeriod(sym, market)]);
      if (aborted.current) return;
      const latestResult = news.results[0];
      const announcedTs = latestResult?.ts || 0;
      const announcedDate = announcedTs ? formatDateOnly(announcedTs) : null;
      setRows((prev) => prev.map((r) => (r.symbol === sym ? { ...r, ...news, resultPeriod: period, resultAnnouncedDate: announcedDate, resultAnnouncedTs: announcedTs, loading: false } : r)));
      if (i < symbolList.length - 1) await new Promise((r) => setTimeout(r, 350));
    }
    setLoadingAll(false);
  }, [symbolList, market]);

  useEffect(() => {
    if (!isOpen) return;
    void runFetch();
    return () => {
      aborted.current = true;
    };
  }, [isOpen, runFetch, refreshKey]);

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
  const totalItems = rows.reduce((s, r) => s + r.results.length + r.general.length, 0);

  return createPortal(
    <div className="news-modal-overlay" onClick={(e) => { if ((e.target as HTMLElement).classList.contains("news-modal-overlay")) onClose(); }}>
      <div className="news-modal" style={{ ["--news-accent" as never]: accentColor }}>
        <div className="news-modal-header">
          <div className="news-modal-title-row">
            <span className="news-modal-emoji">📰</span>
            <div>
              <div className="news-modal-title">{title}</div>
              <div className="news-modal-sub">
                {loadingAll
                  ? `Fetching… ${totalLoaded}/${symbolList.length} symbols`
                  : `${symbolList.length} symbols · ${totalItems} headlines · Results (90d) + News (7d)`}
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
                ? `Result announced ${row.resultAnnouncedDate}${row.resultPeriod ? ` · ${row.resultPeriod}` : ""}`
                : row.resultPeriod
                  ? `Latest reported ${row.resultPeriod}`
                  : "";
              return (
                <article key={row.symbol} className="news-card">
                  <header className="news-card-head">
                    <div className="news-sym">{row.symbol}</div>
                    {announceLabel ? (
                      <div className="news-result-pill" title="Latest result announcement coverage">📅 {announceLabel}</div>
                    ) : !row.loading ? (
                      <div className="news-result-pill news-result-pill--muted">📅 No result data</div>
                    ) : null}
                  </header>

                  {row.loading && <div className="news-card-loading">Loading…</div>}

                  {!row.loading && (
                    <>
                      <section className="news-section">
                        <div className="news-section-title news-section-results">📊 Latest Results Coverage</div>
                        {row.results.length === 0 ? (
                          <div className="news-card-empty">No result-related coverage in last 90 days.</div>
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
                        <div className="news-section-title news-section-general">📰 Recent News (7d)</div>
                        {row.general.length === 0 ? (
                          <div className="news-card-empty">No news-house coverage in last 7 days.</div>
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
