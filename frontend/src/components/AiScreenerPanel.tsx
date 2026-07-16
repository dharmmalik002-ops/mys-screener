import { useState, useEffect, useMemo } from "react";
import {
  type MarketKey,
  runAiScan,
  type AiScanResponse,
  getKnowledgeBase,
  addKnowledgeBaseEntry,
  deleteKnowledgeBaseEntry,
  ingestUrl,
  type KbEntry,
} from "../lib/api";
import { Panel } from "./Panel";
import { ScanTable } from "./ScanTable";

type AiScreenerPanelProps = {
  market: MarketKey;
  onPickSymbol: (symbol: string) => void;
  onRequestAddToWatchlist: (symbol: string) => void;
  onVisibleSymbolsChange?: (symbols: string[]) => void;
  selectedSymbol: string | null;
};

type ScreenerTab = "screener" | "knowledge";
type KbAddMode = "text" | "url";

export function AiScreenerPanel({ market, onPickSymbol, onRequestAddToWatchlist, onVisibleSymbolsChange, selectedSymbol }: AiScreenerPanelProps) {
  const [activeTab, setActiveTab] = useState<ScreenerTab>("screener");

  // ── Screener state ──────────────────────────────────────────────────────────
  const [query, setQuery] = useState("");
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState<string | null>(null);
  const [result, setResult] = useState<AiScanResponse | null>(null);

  const [sortMode, setSortMode] = useState<"change" | "rs">("rs");
  const [arrangementMode, setArrangementMode] = useState<"flat" | "sector">("flat");
  const [sectorSortMode, setSectorSortMode] = useState<"1W" | "1M" | "count-desc" | "count-asc">("count-desc");

  // ── Knowledge Base state ────────────────────────────────────────────────────
  const [kbEntries, setKbEntries] = useState<KbEntry[]>([]);
  const [kbLoading, setKbLoading] = useState(false);
  const [kbError, setKbError] = useState<string | null>(null);
  const [kbAddMode, setKbAddMode] = useState<KbAddMode>("text");
  const [kbTitle, setKbTitle] = useState("");
  const [kbContent, setKbContent] = useState("");
  const [kbUrl, setKbUrl] = useState("");
  const [kbFetchedTitle, setKbFetchedTitle] = useState("");
  const [kbFetchedContent, setKbFetchedContent] = useState("");
  const [kbFetching, setKbFetching] = useState(false);
  const [kbSaving, setKbSaving] = useState(false);

  useEffect(() => {
    if (activeTab === "knowledge") loadKb();
  }, [activeTab]);

  async function loadKb() {
    setKbLoading(true);
    setKbError(null);
    try {
      const data = await getKnowledgeBase();
      setKbEntries(data.entries);
    } catch (err: unknown) {
      setKbError(err instanceof Error ? err.message : "Failed to load knowledge base.");
    } finally {
      setKbLoading(false);
    }
  }

  async function handleFetchUrl() {
    if (!kbUrl.trim()) return;
    setKbFetching(true);
    setKbError(null);
    setKbFetchedTitle("");
    setKbFetchedContent("");
    try {
      const data = await ingestUrl(kbUrl.trim());
      setKbFetchedTitle(data.title);
      setKbFetchedContent(data.content);
    } catch (err: unknown) {
      setKbError(err instanceof Error ? err.message : "Failed to fetch URL.");
    } finally {
      setKbFetching(false);
    }
  }

  async function handleSaveEntry() {
    setKbSaving(true);
    setKbError(null);
    try {
      if (kbAddMode === "text") {
        if (!kbTitle.trim() || !kbContent.trim()) {
          setKbError("Title and content are required.");
          return;
        }
        await addKnowledgeBaseEntry({ type: "text", title: kbTitle.trim(), content: kbContent.trim() });
        setKbTitle("");
        setKbContent("");
      } else {
        const title = kbFetchedTitle || kbUrl;
        const content = kbFetchedContent;
        if (!content.trim()) {
          setKbError("Please fetch the URL first.");
          return;
        }
        await addKnowledgeBaseEntry({
          type: "url",
          title,
          content,
          source_url: kbUrl.trim(),
        });
        setKbUrl("");
        setKbFetchedTitle("");
        setKbFetchedContent("");
      }
      await loadKb();
    } catch (err: unknown) {
      setKbError(err instanceof Error ? err.message : "Failed to save entry.");
    } finally {
      setKbSaving(false);
    }
  }

  async function handleDeleteEntry(id: string) {
    try {
      await deleteKnowledgeBaseEntry(id);
      setKbEntries((prev) => prev.filter((e) => e.id !== id));
    } catch {
      // ignore
    }
  }

  const handleSubmit = async (e: React.FormEvent) => {
    e.preventDefault();
    if (!query.trim()) return;

    setLoading(true);
    setError(null);
    try {
      const response = await runAiScan(query, market);
      setResult(response);
    } catch (err: any) {
      setError(err.message || "Failed to execute AI scan.");
    } finally {
      setLoading(false);
    }
  };

  const sortedItems = useMemo(() => {
    if (!result) return [];
    return [...result.results.items].sort((left, right) => {
      if (sortMode === "change") {
        return right.change_pct - left.change_pct;
      }
      return (right.rs_rating ?? Number.NEGATIVE_INFINITY) - (left.rs_rating ?? Number.NEGATIVE_INFINITY);
    });
  }, [result, sortMode]);

  useEffect(() => {
    if (onVisibleSymbolsChange) {
      onVisibleSymbolsChange(sortedItems.map(s => s.symbol));
    }
  }, [sortedItems, onVisibleSymbolsChange]);

  return (
    <div className="screener-main-stack" style={{ paddingTop: '20px' }}>
      {/* Tab switcher */}
      <div style={{ display: 'flex', gap: 4, padding: '0 20px 12px' }}>
        <button
          type="button"
          className={activeTab === "screener" ? "timeframe-pill active" : "timeframe-pill"}
          onClick={() => setActiveTab("screener")}
        >
          ✦ AI Screener
        </button>
        <button
          type="button"
          className={activeTab === "knowledge" ? "timeframe-pill active" : "timeframe-pill"}
          onClick={() => setActiveTab("knowledge")}
        >
          📚 Knowledge Base
        </button>
      </div>

      {/* ── AI Screener tab ─────────────────────────────────────────────────── */}
      {activeTab === "screener" ? (
        <>
          <Panel title="AI Screener" subtitle="Describe the kinds of stocks you want using natural language." actions={null}>
            <form onSubmit={handleSubmit} style={{ padding: '0 20px 20px', display: 'flex', gap: '10px' }}>
              <input
                type="text"
                value={query}
                onChange={(e) => setQuery(e.target.value)}
                placeholder="E.g., RVOL > 3 and change > 6% on April 7 2026 · stocks with highest quarterly volume in last 7 days · EPS growth > 30% with RS > 80 near 52W high..."
                style={{ flex: 1, padding: '12px', borderRadius: '8px', border: '1px solid var(--surface1)', background: 'var(--mantle)', color: 'var(--text)', fontSize: '1rem' }}
              />
              <button type="submit" className="nav-button primary ai-sparkle" disabled={loading || !query.trim()} style={{ whiteSpace: 'nowrap', padding: '0 20px', borderRadius: '8px' }}>
                {loading ? "Thinking..." : "Scan with AI"}
              </button>
            </form>
            {error ? <div className="error-message" style={{ margin: '0 20px 20px', color: 'var(--red)' }}>{error}</div> : null}
            {result ? (
              <div style={{ padding: '0 20px 20px' }}>
                 <div style={{ padding: '12px', background: 'var(--surface0)', borderRadius: '8px', border: '1px solid var(--surface1)' }}>
                   <span style={{ color: 'var(--subtext0)', fontSize: '0.85rem', textTransform: 'uppercase', letterSpacing: '0.5px' }}>Parameters Found:</span>
                   <div style={{ display: 'flex', gap: '8px', flexWrap: 'wrap', marginTop: '8px' }}>
                     {result.parsed_request.scan_date ? (
                       <span style={{ background: 'var(--teal-soft)', border: '1px solid color-mix(in srgb, var(--teal) 40%, transparent)', padding: '4px 10px', borderRadius: '6px', fontSize: '0.85rem', color: 'var(--teal)', fontWeight: 700 }}>
                         Date: {String(result.parsed_request.scan_date)}
                       </span>
                     ) : null}
                     {result.parsed_request.highest_vol_lookback_days ? (
                       <span style={{ background: 'var(--orange-soft)', border: '1px solid color-mix(in srgb, var(--orange) 40%, transparent)', padding: '4px 10px', borderRadius: '6px', fontSize: '0.85rem', color: 'var(--orange)', fontWeight: 700 }}>
                         Quarterly-High Vol: last {result.parsed_request.highest_vol_lookback_days} days
                       </span>
                     ) : null}
                     {result.parsed_request.min_relative_volume != null ? (
                       <span style={{ background: 'var(--pink-soft)', border: '1px solid color-mix(in srgb, var(--pink) 40%, transparent)', padding: '4px 10px', borderRadius: '6px', fontSize: '0.85rem', color: 'var(--pink)', fontWeight: 700 }}>
                         RVOL ≥ {result.parsed_request.min_relative_volume}x
                       </span>
                     ) : null}
                     {result.parsed_request.min_change_pct != null ? (
                       <span style={{ background: 'var(--positive-soft)', border: '1px solid color-mix(in srgb, var(--positive) 40%, transparent)', padding: '4px 10px', borderRadius: '6px', fontSize: '0.85rem', color: 'var(--positive)', fontWeight: 700 }}>
                         Chg ≥ {result.parsed_request.min_change_pct}%
                       </span>
                     ) : null}
                     {Object.entries(result.parsed_request)
                       .filter(([k, v]) =>
                         v !== null && v !== false &&
                         k !== 'market' && k !== 'limit' &&
                         k !== 'scan_date' && k !== 'highest_vol_lookback_days' &&
                         k !== 'min_relative_volume' && k !== 'min_change_pct' &&
                         k !== 'sort_by' && k !== 'sort_order' && k !== 'pattern' &&
                         k !== 'price_vs_ma_mode' && k !== 'price_vs_ma_key' &&
                         k !== 'price_to_ma_key' && k !== 'return_period'
                       )
                       .map(([k, v]) => (
                         <span key={k} style={{ background: 'var(--surface1)', padding: '4px 8px', borderRadius: '4px', fontSize: '0.85rem', color: 'var(--text)' }}>
                           <strong>{k}</strong>: {typeof v === 'boolean' ? 'Yes' : String(v)}
                         </span>
                       ))}
                     {Object.entries(result.parsed_request).filter(([k, v]) => v !== null && v !== false && k !== 'market' && k !== 'limit').length === 0 && (
                       <span style={{ color: 'var(--subtext1)', fontSize: '0.85rem' }}>No specific filters parsed. Using default universe.</span>
                     )}
                   </div>
                 </div>
              </div>
            ) : null}
          </Panel>

          {result ? (
            <ScanTable
              market={market}
              loading={loading}
              sectorSummaryLoading={false}
              scan={result.results.scan}
              items={sortedItems}
              sectorSummaries={result.results.sector_summaries}
              onPickSymbol={onPickSymbol}
              onRequestAddToWatchlist={onRequestAddToWatchlist}
              selectedSymbol={selectedSymbol}
              sortMode={sortMode}
              onSortModeChange={setSortMode}
              arrangementMode={arrangementMode}
              onArrangementModeChange={setArrangementMode}
              sectorSortMode={sectorSortMode}
              onSectorSortModeChange={setSectorSortMode}
              onExport={() => {}}
            />
          ) : (
             <div className="empty-state" style={{ marginTop: '40px' }}>
                <p>Ask the AI to find exactly what you're looking for.</p>
                <p style={{ color: 'var(--subtext0)', fontSize: '0.9rem', marginTop: '8px' }}>
                  Try: <em>"RVOL &gt; 3 and change &gt; 6% on April 7 2026"</em> · <em>"highest quarterly volume in last 7 days"</em> · <em>"EPS growth &gt; 30% with RS &gt; 80"</em>
                </p>
             </div>
          )}
        </>
      ) : null}

      {/* ── Knowledge Base tab ──────────────────────────────────────────────── */}
      {activeTab === "knowledge" ? (
        <div style={{ padding: '0 20px 40px', display: 'flex', flexDirection: 'column', gap: 24 }}>
          <Panel title="Knowledge Base" subtitle="Teach the AI your trading principles. It will apply them when analysing charts." actions={null}>
            <div style={{ padding: '0 20px 20px' }}>
              {/* Add mode selector */}
              <div style={{ display: 'flex', gap: 6, marginBottom: 16 }}>
                <button
                  type="button"
                  className={kbAddMode === "text" ? "timeframe-pill active" : "timeframe-pill"}
                  onClick={() => setKbAddMode("text")}
                >
                  ✏ Add Note
                </button>
                <button
                  type="button"
                  className={kbAddMode === "url" ? "timeframe-pill active" : "timeframe-pill"}
                  onClick={() => setKbAddMode("url")}
                >
                  🔗 Add from URL / YouTube
                </button>
              </div>

              {/* Text note form */}
              {kbAddMode === "text" ? (
                <div style={{ display: 'flex', flexDirection: 'column', gap: 10 }}>
                  <input
                    type="text"
                    value={kbTitle}
                    onChange={(e) => setKbTitle(e.target.value)}
                    placeholder="Title (e.g. Breakout Rules, Volume Principles, RS Trading)"
                    style={{ padding: '10px 12px', borderRadius: 8, border: '1px solid var(--glass-border)', background: 'var(--surface)', color: 'var(--text)', fontSize: '0.9rem', outline: 'none' }}
                  />
                  <textarea
                    value={kbContent}
                    onChange={(e) => setKbContent(e.target.value)}
                    placeholder="Write your trading principle, rule, or insight here…"
                    rows={5}
                    style={{ padding: '10px 12px', borderRadius: 8, border: '1px solid var(--glass-border)', background: 'var(--surface)', color: 'var(--text)', fontSize: '0.875rem', resize: 'vertical', fontFamily: 'inherit', outline: 'none', lineHeight: 1.6 }}
                  />
                  <button
                    type="button"
                    className="nav-button primary"
                    disabled={kbSaving || !kbTitle.trim() || !kbContent.trim()}
                    onClick={handleSaveEntry}
                    style={{ alignSelf: 'flex-start', padding: '8px 20px', borderRadius: 8 }}
                  >
                    {kbSaving ? "Saving…" : "Save to Knowledge Base"}
                  </button>
                </div>
              ) : (
                /* URL / YouTube form */
                <div style={{ display: 'flex', flexDirection: 'column', gap: 10 }}>
                  <div style={{ display: 'flex', gap: 8 }}>
                    <input
                      type="url"
                      value={kbUrl}
                      onChange={(e) => setKbUrl(e.target.value)}
                      placeholder="https://www.youtube.com/watch?v=... or any blog / article URL"
                      style={{ flex: 1, padding: '10px 12px', borderRadius: 8, border: '1px solid var(--glass-border)', background: 'var(--surface)', color: 'var(--text)', fontSize: '0.875rem', outline: 'none' }}
                    />
                    <button
                      type="button"
                      className="nav-button"
                      disabled={kbFetching || !kbUrl.trim()}
                      onClick={handleFetchUrl}
                      style={{ whiteSpace: 'nowrap', padding: '0 16px', borderRadius: 8 }}
                    >
                      {kbFetching ? "Fetching…" : "Fetch Content"}
                    </button>
                  </div>
                  {kbFetchedContent ? (
                    <div style={{ padding: 12, borderRadius: 8, border: '1px solid var(--glass-border)', background: 'var(--surface-soft)', fontSize: '0.82rem', color: 'var(--text-muted)', lineHeight: 1.6, maxHeight: 160, overflowY: 'auto' }}>
                      <strong style={{ color: 'var(--text)', display: 'block', marginBottom: 6 }}>{kbFetchedTitle}</strong>
                      {kbFetchedContent.slice(0, 600)}{kbFetchedContent.length > 600 ? '…' : ''}
                    </div>
                  ) : null}
                  {kbFetchedContent ? (
                    <button
                      type="button"
                      className="nav-button primary"
                      disabled={kbSaving}
                      onClick={handleSaveEntry}
                      style={{ alignSelf: 'flex-start', padding: '8px 20px', borderRadius: 8 }}
                    >
                      {kbSaving ? "Saving…" : "Save to Knowledge Base"}
                    </button>
                  ) : null}
                </div>
              )}

              {kbError ? (
                <div style={{ marginTop: 10, color: 'var(--red)', fontSize: '0.875rem' }}>{kbError}</div>
              ) : null}
            </div>
          </Panel>

          {/* Entry list */}
          <Panel title={`Saved Entries (${kbEntries.length})`} subtitle="The AI reads all these when analysing your charts." actions={null}>
            <div style={{ padding: '0 20px 20px', display: 'flex', flexDirection: 'column', gap: 10 }}>
              {kbLoading ? (
                <div style={{ color: 'var(--text-muted)', fontSize: '0.875rem' }}>Loading…</div>
              ) : kbEntries.length === 0 ? (
                <div style={{ color: 'var(--text-muted)', fontSize: '0.875rem', fontStyle: 'italic' }}>
                  No entries yet. Add your first trading principle above.
                </div>
              ) : (
                kbEntries.map((entry) => (
                  <div
                    key={entry.id}
                    style={{
                      padding: '12px 14px',
                      borderRadius: 10,
                      background: 'var(--surface-soft)',
                      border: '1px solid var(--glass-border)',
                      display: 'flex',
                      gap: 12,
                    }}
                  >
                    <div style={{ flex: 1, minWidth: 0 }}>
                      <div style={{ display: 'flex', alignItems: 'center', gap: 8, marginBottom: 4 }}>
                        <span style={{ fontSize: '0.8rem' }}>
                          {entry.type === 'youtube' ? '▶' : entry.type === 'url' ? '🔗' : '✏'}
                        </span>
                        <strong style={{ fontSize: '0.88rem', color: 'var(--text)' }}>{entry.title}</strong>
                        <span style={{ fontSize: '0.72rem', color: 'var(--text-muted)', marginLeft: 'auto', whiteSpace: 'nowrap' }}>
                          {entry.content_length.toLocaleString()} chars
                        </span>
                      </div>
                      <div style={{ fontSize: '0.78rem', color: 'var(--text-muted)', lineHeight: 1.5 }}>
                        {entry.content_preview}
                      </div>
                      {entry.source_url ? (
                        <a
                          href={entry.source_url}
                          target="_blank"
                          rel="noopener noreferrer"
                          style={{ fontSize: '0.72rem', color: 'var(--accent)', marginTop: 4, display: 'block', overflow: 'hidden', textOverflow: 'ellipsis', whiteSpace: 'nowrap' }}
                        >
                          {entry.source_url}
                        </a>
                      ) : null}
                    </div>
                    <button
                      type="button"
                      onClick={() => handleDeleteEntry(entry.id)}
                      style={{
                        background: 'none',
                        border: '1px solid var(--glass-border)',
                        color: 'var(--red)',
                        cursor: 'pointer',
                        padding: '4px 10px',
                        borderRadius: 6,
                        fontSize: '0.75rem',
                        flexShrink: 0,
                        alignSelf: 'flex-start',
                      }}
                      title="Delete this entry"
                    >
                      ✕
                    </button>
                  </div>
                ))
              )}
            </div>
          </Panel>
        </div>
      ) : null}
    </div>
  );
}
