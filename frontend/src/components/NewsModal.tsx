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
type SymbolNews = { symbol: string; items: NewsItem[]; latestResultPeriod: string | null; latestResultRaw: string | null; loading: boolean; error: string | null };

const CORP_ANNOUNCE_PATTERNS = [
  /\bcorporate announcement\b/i,
  /\boutcome of board meeting\b/i,
  /\binsider trading\b/i,
  /\bdisclosure under\b/i,
  /\bdisclosures under\b/i,
  /\bsubmission of\b/i,
  /\bcompliance certificate\b/i,
  /\bpostal ballot\b/i,
  /\bpledge\b/i,
  /\bsast\b/i,
  /\bregulation 30\b/i,
  /\bregulation 7\(2\)\b/i,
  /\bbse\s*:\s*5\d{5}\b/i,
  /\bnse\s*:\s*[A-Z]+\b/i,
  /\bvotes? cast\b/i,
];

const CORP_SOURCE_PATTERNS = [
  /\bbse\s*india\b/i,
  /\bnse\s*india\b/i,
  /\bbseindia\.com\b/i,
  /\bnseindia\.com\b/i,
];

function isCorporateAnnouncement(title: string, source: string): boolean {
  const haystack = `${title} ${source}`;
  if (CORP_SOURCE_PATTERNS.some((re) => re.test(source))) return true;
  return CORP_ANNOUNCE_PATTERNS.some((re) => re.test(haystack));
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

async function fetchSymbolNews(symbol: string): Promise<NewsItem[]> {
  const cb = Date.now();
  const company = symbol.replace(/\.(NS|BO)$/i, "");
  const q = encodeURIComponent(`${company} stock OR shares -site:bseindia.com -site:nseindia.com when:7d`);
  const feedUrl = `https://news.google.com/rss/search?q=${q}&hl=en-IN&gl=IN&ceid=IN:en&cb=${cb}`;
  try {
    const res = await fetch(`https://api.rss2json.com/v1/api.json?rss_url=${encodeURIComponent(feedUrl)}`);
    const data = await res.json();
    if (data.status !== "ok" || !data.items?.length) return [];
    type RawRssItem = { title?: string; link?: string; description?: string; pubDate?: string; author?: string };
    const cleaned = (data.items as RawRssItem[])
      .map((item) => {
        const title = (item.title || "").trim();
        const sourceMatch = title.match(/-\s*([^-]+)$/);
        const source = sourceMatch ? sourceMatch[1].trim() : (item.author || "");
        const titleClean = sourceMatch ? title.replace(/\s*-\s*[^-]+$/, "").trim() : title;
        const summary = cleanSummary(item.description || "") || "Click to read full article.";
        const dateInfo = parseDate(item.pubDate);
        return { title: titleClean, link: item.link || "", summary, date: dateInfo.display, source, ts: dateInfo.ts };
      })
      .filter((x) => x.title && !isCorporateAnnouncement(x.title, x.source))
      .sort((a, b) => b.ts - a.ts)
      .slice(0, 6);
    return cleaned;
  } catch {
    return [];
  }
}

async function fetchLatestResult(symbol: string, market: MarketKey): Promise<string | null> {
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
    setRows(symbolList.map((s) => ({ symbol: s, items: [], latestResultPeriod: null, latestResultRaw: null, loading: true, error: null })));
    for (let i = 0; i < symbolList.length; i++) {
      if (aborted.current) return;
      const sym = symbolList[i];
      const [items, period] = await Promise.all([fetchSymbolNews(sym), fetchLatestResult(sym, market)]);
      if (aborted.current) return;
      setRows((prev) => prev.map((r) => (r.symbol === sym ? { ...r, items, latestResultPeriod: period, latestResultRaw: period, loading: false, error: items.length === 0 ? "No recent news." : null } : r)));
      if (i < symbolList.length - 1) await new Promise((r) => setTimeout(r, 450));
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
  const totalItems = rows.reduce((s, r) => s + r.items.length, 0);

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
                  : `${symbolList.length} symbols · ${totalItems} headlines · Latest 7 days`}
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
            {rows.map((row) => (
              <article key={row.symbol} className="news-card">
                <header className="news-card-head">
                  <div className="news-sym">{row.symbol}</div>
                  {row.latestResultPeriod ? (
                    <div className="news-result-pill" title="Latest reported result period">📅 Result: {row.latestResultPeriod}</div>
                  ) : !row.loading ? (
                    <div className="news-result-pill news-result-pill--muted">📅 Result: —</div>
                  ) : null}
                </header>
                {row.loading && <div className="news-card-loading">Loading…</div>}
                {!row.loading && row.items.length === 0 && (
                  <div className="news-card-empty">No recent news houses coverage in last 7 days.</div>
                )}
                {!row.loading && row.items.length > 0 && (
                  <ul className="news-list">
                    {row.items.map((item, i) => (
                      <li key={i} className="news-item">
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
              </article>
            ))}
          </div>
        </div>
      </div>
    </div>,
    document.body,
  );
}
